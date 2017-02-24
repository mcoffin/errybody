module Errybody.Config where

import Prelude
import Data.Options (Options)
import Mesos.Raw (FrameworkID, TaskInfo)
import Node.HTTP.Client (RequestOptions)

type Config =
    { masterRequestOptions :: Options RequestOptions
    , streamId :: String
    , frameworkId :: FrameworkID
    , baseTaskInfo :: TaskInfo
    }
