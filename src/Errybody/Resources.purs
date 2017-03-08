module Errybody.Resources where

import Prelude
import Control.Monad.Rec.Class (Step(..), tailRecM2)
import Data.Array as A
import Data.Foldable (and, elem)
import Data.Identity (Identity(..))
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Newtype (unwrap)
import Data.StrMap as SM
import Data.Tuple (Tuple(..))
import Mesos.Raw (Range(..), Ranges(..), Resource(..), ResourceValue(..), Set(..))

rangeContainedBy :: Range -> Ranges -> Boolean
rangeContainedBy origReq (Ranges avail) = unwrap $ f origReq 0 where
    rangeMatches :: Range -> Range -> Maybe Range
    rangeMatches (Range req) (Range r)
      | req.begin >= r.begin && req.end <= r.end = Nothing
      | req.begin < r.begin && req.end <= r.end = Just <<< Range $ req { end = r.begin - 1 }
      | req.begin >= r.begin && req.end > r.end = Just <<< Range $ req { begin = r.end + 1 }
      | otherwise = pure $ Range req
    f = tailRecM2 \req idx -> Identity $
        case A.index avail idx of
          Just toCheck ->
              case rangeMatches req toCheck of
                Just nextReq -> Loop { a: nextReq, b: idx + 1 }
                Nothing -> Done true
          Nothing -> Done false

resourceContainedBy :: ResourceValue -> ResourceValue -> Boolean
resourceContainedBy (ScalarResource req) (ScalarResource avail) = req <= avail
resourceContainedBy (SetResource (Set req)) (SetResource (Set avail)) = and $ flip elem avail <$> req
resourceContainedBy (RangesResource (Ranges req)) (RangesResource avail) = and $ flip rangeContainedBy avail <$> req
resourceContainedBy _ _ = false

resourcesContain :: Array Resource -> Array Resource -> Boolean
resourcesContain requirements available = and $ isSatisfied <$> requirements where
    availableMap = SM.fromFoldable $ available <#> \(Resource k v) -> Tuple k v
    isSatisfied :: Resource -> Boolean
    isSatisfied (Resource name value) = fromMaybe false $ SM.lookup name availableMap <#> resourceContainedBy value
