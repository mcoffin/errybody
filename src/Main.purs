module Main where

import Prelude
import Control.Alt ((<|>))
import Control.Monad.Aff (Aff, runAff)
import Control.Monad.Eff.Class (liftEff)
import Control.Monad.Eff.Console (log)
import Control.Monad.Eff.Exception (throw, throwException)
import Control.Monad.Error.Class (class MonadError, throwError)
import Control.Monad.Except (runExcept)
import Control.Monad.State.Trans (execStateT, modify)
import Control.Monad.Trans.Class (lift)
import Data.Either (either)
import Data.Foldable (fold)
import Data.Foreign (parseJSON)
import Data.Foreign.Class (read)
import Data.Int as I
import Data.Maybe (Maybe(..), fromMaybe, maybe')
import Data.Monoid (mempty)
import Data.Nullable (toMaybe)
import Data.Options (Options, (:=))
import Data.StrMap (StrMap, lookup)
import Data.StrMap as SM
import Data.Tuple (Tuple(..))
import Errybody.Scheduler as Sched
import Mesos.Raw (FrameworkInfo(..), Value(..), TaskInfo)
import Mesos.Scheduler (Subscribe(..), Subscribed(..), subscribe')
import Mesos.Util (throwErrorS)
import Minimist (Arg(..), MinimistOptions, aliases, interpretAsStrings, parseArgs)
import Node.Encoding as Encoding
import Node.FS (FS)
import Node.FS.Aff (readTextFile)
import Node.HTTP.Client (RequestOptions, auth, protocol, port, hostname)
import Node.Process (argv)
import Node.URL as URL

minimistOptions :: Options MinimistOptions
minimistOptions = fold $
    [ interpretAsStrings := ["master", "principal", "secret", "user", "framework-name", "hostname"]
    , aliases := SM.singleton "t" ["task"]
    ]

data InitializationError = ErrorAtArgument String InitializationError
                         | WrongArgumentType String
                         | RequiredArgument String
                         | InitializationError String

instance initializationErrorShow :: Show InitializationError where
    show (ErrorAtArgument name next) = "Error with argument \"" <> name <> "\": " <> show next
    show (WrongArgumentType expectedType) = "Expected argument type \"" <> expectedType <> "\""
    show (RequiredArgument name) = "Argument " <> show name <> " is required"
    show (InitializationError msg) = msg

stringArg :: Arg -> Maybe String
stringArg (ArgString s) = Just s
stringArg _ = Nothing

defaultProtocolPort :: String -> Maybe Int
defaultProtocolPort "http:" = Just 80
defaultProtocolPort "https:" = Just 443
defaultProtocolPort _ = Nothing

subscribeOptions :: forall m. (MonadError InitializationError m) => StrMap Arg -> m (Options RequestOptions)
subscribeOptions args = flip execStateT mempty do
    maybeMasterAuth <- lift authString
    case maybeMasterAuth of
      Just masterAuth -> setOption $ auth := masterAuth
      Nothing -> pure unit
    Tuple _ masterUrlArg <- lift $ manditoryArg "master"
    masterUrlStr <- lift (maybe' (\_ -> throwError $ ErrorAtArgument "master" (WrongArgumentType "string")) pure $ stringArg masterUrlArg)
    let masterUrl = URL.parse masterUrlStr
    masterProtocol <- lift (maybe' (\_ -> throwError $ ErrorAtArgument "master" (InitializationError "couldn't discern protocol")) pure $ toMaybe masterUrl.protocol)
    setOption $ protocol := masterProtocol
    masterPort <- lift (maybe' (\_ -> throwError $ ErrorAtArgument "master" (InitializationError "couldn't discern port from protocol")) pure $ (toMaybe masterUrl.port >>= I.fromString) <|> defaultProtocolPort masterProtocol)
    setOption $ port := masterPort
    masterHostname <- lift (maybe' (\_ -> throwError $ ErrorAtArgument "master" (InitializationError "couldn't discern hostname")) pure $ toMaybe masterUrl.hostname)
    setOption $ hostname := masterHostname
    where
        setOption o = modify $ flip (<>) o
        manditoryArg :: String -> m (Tuple String Arg)
        manditoryArg key = Tuple key <$> maybe' e pure (lookup key args) where
            e _ = throwError $ RequiredArgument key
        authString :: m (Maybe String)
        authString = do
            case Tuple (lookup "principal" args >>= stringArg) (lookup "secret" args >>= stringArg) of
              Tuple (Just p) (Just s) -> pure <<< Just $ p <> ":" <> s
              Tuple Nothing Nothing -> pure Nothing
              Tuple (Just _) Nothing -> throwError $ ErrorAtArgument "principal" (RequiredArgument "secret")
              Tuple Nothing (Just _) -> throwError $ ErrorAtArgument "secret" (RequiredArgument "principal")

valueOf :: forall a. Value a -> a
valueOf (Value a) = a

defaultUser :: String
defaultUser = "root"

defaultName :: String
defaultName = "errybody"

frameworkInfo :: forall m. (MonadError InitializationError m) => StrMap Arg -> m FrameworkInfo
frameworkInfo args = pure <<< FrameworkInfo $
    { user: fromMaybe defaultUser userOpt
    , name: fromMaybe defaultName frameworkNameOpt
    , id: Nothing
    , failoverTimeout: Nothing
    , checkpoint: Nothing
    , role: Nothing
    , hostname: userHostname
    , principal: userPrincipal
    , webuiUrl: Nothing
    , capabilities: []
    , labels: Nothing
    } where
        userOpt = lookup "user" args >>= stringArg
        frameworkNameOpt = lookup "framework-name" args >>= stringArg
        userPrincipal = lookup "principal" args >>= stringArg
        userHostname = lookup "hostname" args >>= stringArg

getTaskInfo :: forall eff. StrMap Arg -> Aff (fs :: FS | eff) TaskInfo
getTaskInfo args = do
    path <- maybe' (\_ -> throwErrorS "You must supply a task") pure $ lookup "task" args >>= stringArg
    jsonTaskInfo <- readTextFile Encoding.UTF8 path
    let info = runExcept $ parseJSON jsonTaskInfo >>= read
    either (throwErrorS <<< show) pure info

main = do
    args <- flip parseArgs minimistOptions <$> argv
    subscribeOpts <- either (throw <<< show) pure $ runExcept (subscribeOptions args)
    fwinfo <- either (throw <<< show) pure $ runExcept (frameworkInfo args)
    runAff throwException (\_ -> pure unit) do
        taskInfo <- getTaskInfo args
        Tuple streamId v <- subscribe' subscribeOpts (Subscribe { frameworkInfo: fwinfo })
        (Subscribed subscribedInfo) <- Sched.waitForSubscription v
        let cfg =
                { masterRequestOptions: subscribeOpts
                , streamId: streamId
                , frameworkId: subscribedInfo.frameworkId
                , baseTaskInfo: taskInfo
                }
        liftEff <<< log $ "framework-id: " <> valueOf subscribedInfo.frameworkId
        Sched.handleMessages cfg v
    pure unit
