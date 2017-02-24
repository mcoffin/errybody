module Errybody.UUID
    ( GENUUID
    , UUIDVersion(..)
    , genUUID
    ) where

import Prelude
import Control.Monad.Eff (Eff)

foreign import data GENUUID :: !

data UUIDVersion = UUIDV1
                 | UUIDV4

genUUID :: forall eff. UUIDVersion -> Eff (genuuid :: GENUUID | eff) String
genUUID = genUUIDImpl <<< uuidVersionString

uuidVersionString :: UUIDVersion -> String
uuidVersionString UUIDV1 = "v1"
uuidVersionString UUIDV4 = "v4"

foreign import genUUIDImpl :: forall eff. String -> Eff (genuuid :: GENUUID | eff) String
