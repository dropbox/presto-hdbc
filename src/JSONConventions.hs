module JSONConventions where

import Control.Lens ((%~), _head)
import Data.Aeson.TH (Options(..), SumEncoding(..), defaultOptions)
import Data.Char (toLower)

jsonOptions = defaultOptions { constructorTagModifier = _head %~ toLower
                             , fieldLabelModifier = \s -> let d = dropWhile (/= '_') s in
                                                            if null d then s else (tail d)
                             , omitNothingFields = True
                             , sumEncoding = ObjectWithSingleField
                             }
