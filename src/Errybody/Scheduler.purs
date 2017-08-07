module Errybody.Scheduler where

import Prelude
import Control.Monad.Aff (Aff)
import Control.Monad.Aff.AVar (AVAR, AVar, takeVar)
import Control.Monad.Eff.Class (liftEff)
import Control.Monad.Eff.Console (CONSOLE, logShow)
import Control.Monad.Eff.Exception (EXCEPTION)
import Control.Monad.Rec.Class (Step(..), forever, tailRecM)
import Control.Monad.State.Class (class MonadState)
import Control.Monad.State.Trans (StateT, evalStateT, gets, modify)
import Control.Monad.Trans.Class (lift)
import Data.Array (filter)
import Data.Array as A
import Data.Foldable (find)
import Data.Maybe (Maybe(..), isJust)
import Data.Monoid (mempty)
import Data.Newtype (unwrap)
import Data.StrMap (StrMap)
import Data.StrMap as SM
import Data.Traversable (for)
import Data.Tuple (Tuple(..), snd)
import Errybody.Config (Config)
import Errybody.Resources (resourcesContain)
import Errybody.UUID (GENUUID, UUIDVersion(..), genUUID)
import Mesos.Raw (AgentID, TaskID, Filters(..), TaskStatus(..), Offer(..), Value(..), TaskInfo(..))
import Mesos.Raw.Offer (Operation(..))
import Mesos.Scheduler (Accept(..), Message(..), Subscribed, Acknowlege(..), ReconcileTask(..), accept)
import Mesos.Util (throwErrorS)
import Node.HTTP as HTTP
import Node.HTTP.Client (Response)

type SchedulerState =
    { tasks :: StrMap ReconcileTask
    , slaves :: StrMap TaskStatus
    }

initialState :: SchedulerState
initialState = { tasks: SM.empty
               , slaves: SM.empty
               }

initialTaskStatus :: TaskID -> Maybe AgentID -> TaskStatus
initialTaskStatus taskId slaveId = TaskStatus $
    { taskId: taskId
    , state: "TASK_STAGING"
    , message: Nothing
    , source: Nothing
    , reason: Nothing
    , data: Nothing
    , slaveId: slaveId
    , executorId: Nothing
    , timestamp: Nothing
    , uuid: Nothing
    , healthy: Nothing
    , labels: Nothing
    , containerStatus: Nothing
    }

registerTask :: ∀ m. (MonadState SchedulerState m) => ReconcileTask -> m Unit
registerTask (ReconcileTask t) = do
    modify \origState -> origState { tasks = SM.insert (unwrap t.taskId) (ReconcileTask t) origState.tasks }
    case t.slaveId of
      Just slaveId ->
          modify \origState -> origState { slaves = SM.insert (unwrap slaveId) (initialTaskStatus t.taskId t.slaveId) origState.slaves }
      Nothing -> pure unit

waitForSubscription :: ∀ eff. AVar Message -> Aff (avar :: AVAR, console :: CONSOLE | eff) Subscribed
waitForSubscription v = waitForSubscriptionImpl unit where
    waitForSubscriptionImpl = tailRecM \_ -> do
        msg <- takeVar v
        case msg of
          SubscribedMessage subscribedInfo -> pure <<< Done $ subscribedInfo
          _ -> pure $ Loop unit

handleMessages :: ∀ eff. Config -> AVar Message -> Aff (err :: EXCEPTION, http :: HTTP.HTTP, avar :: AVAR, console :: CONSOLE, genuuid :: GENUUID | eff) Unit
handleMessages cfg v = flip evalStateT initialState $ forever do
    msg <- lift $ takeVar v
    lift <<< liftEff $ logShow msg
    case msg of
      OffersMessage offers -> handleOffers offers
      UpdateMessage status -> handleUpdate status
      _ -> pure unit
    where
        accept' :: ∀ eff0. Message -> Aff (err :: EXCEPTION, http :: HTTP.HTTP, avar :: AVAR, console :: CONSOLE | eff0) Response
        accept' message = do
            liftEff $ logShow message
            accept cfg.masterRequestOptions cfg.streamId message
        handleUpdate :: ∀ eff0. TaskStatus -> StateT SchedulerState (Aff (err :: EXCEPTION, http :: HTTP.HTTP, avar :: AVAR, console :: CONSOLE, genuuid :: GENUUID | eff0)) Unit
        handleUpdate (TaskStatus status) = do
            maybeTask <- SM.lookup (unwrap status.taskId) <$> gets \s -> s.tasks
            let maybeTaskId = maybeTask >>= \(ReconcileTask task) -> task.slaveId
            case maybeTaskId of
              Just taskId -> do
                  modify \origState -> origState { slaves = SM.insert (unwrap taskId) (TaskStatus status) origState.slaves }
                  pure unit
              Nothing -> pure unit
            let maybeAcknowlegeMessage =
                    do
                        ReconcileTask task <- maybeTask
                        slaveId <- task.slaveId
                        uuid <- status.uuid
                        pure <<< AcknowlegeMessage cfg.frameworkId <<< Acknowlege $
                            { taskId: status.taskId
                            , slaveId: slaveId
                            , uuid: uuid
                            }
            case maybeAcknowlegeMessage of
              Just acknowlegeMessage -> do
                  lift $ accept' acknowlegeMessage
                  pure unit
              Nothing -> pure unit
        handleOffers :: ∀ eff0. Array Offer -> StateT SchedulerState (Aff (err :: EXCEPTION, http :: HTTP.HTTP, avar :: AVAR, console :: CONSOLE, genuuid :: GENUUID | eff0)) Unit
        handleOffers offers = do
            beforeTasks <- gets (\s -> s.slaves)
            let baseTaskInfo = unwrap cfg.baseTaskInfo
                relevantOffers =
                    flip filter offers \(Offer offer) ->
                        case (\(TaskStatus status) -> status.state) <$> SM.lookup (unwrap offer.slaveId) beforeTasks of
                          Just "TASK_STAGING" -> false
                          Just "TASK_STARTING" -> false
                          Just "TASK_RUNNING" -> false
                          _ -> resourcesContain baseTaskInfo.resources offer.resources
            taskInfoTuples <- lift <<< liftEff $ for relevantOffers \(Offer offer) -> do
                taskId <- genUUID UUIDV4
                taskInfo <- pure <<< TaskInfo $
                    baseTaskInfo { taskId = Value taskId
                                 , slaveId = offer.slaveId
                                 }
                pure $ Tuple offer.id taskInfo
            let taskInfos = snd <$> taskInfoTuples
            for (taskInfos <#> \(TaskInfo info) -> ReconcileTask { taskId: info.taskId, slaveId: Just info.slaveId }) registerTask
            let acceptMessages =
                    (AcceptMessage cfg.frameworkId <<< Accept) <$> makeAccept <$> taskInfoTuples
                relevantOfferIds = relevantOffers <#> \(Offer offer) -> offer.id
                irrelevantOffers = flip filter offers \(Offer offer) -> not isJust $ find (eq offer.id) relevantOfferIds
                rejectMessage = AcceptMessage cfg.frameworkId $ Accept { offerIds: irrelevantOffers <#> \(Offer offer) -> offer.id
                                                                        , operations: mempty
                                                                        , filters: Just $ Filters { refuseSeconds: Just 10.0 }
                                                                        }
            lift $ for (acceptMessages <> A.singleton rejectMessage) accept'
            pure unit
            where
                offerIds = offers <#> \(Offer o) -> o.id
                makeAccept (Tuple offerId taskInfo) =
                    { offerIds: A.singleton offerId
                    , operations: A.singleton <<< LaunchOperation <<< A.singleton $ taskInfo
                    , filters: Just $ Filters { refuseSeconds: Just 10.0 }
                    }
