{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
module Database.HDBC.PrestoData where

import Data.Aeson
import GHC.Generics
-- For pretty-printing
import Data.Aeson.Encode.Pretty

import Util.JSONConventions
import Data.Aeson.TH

import Control.Monad
import Data.Text as T

import Data.Scientific

import Database.HDBC (SqlValue)

data PrestoStats = PrestoStats { state :: String
                               , scheduled :: Bool
                               , nodes :: Integer
                               , totalSplits :: Integer
                               , queuedSplits :: Integer
                               , runningSplits :: Integer
                               , completedSplits :: Integer
                               , userTimeMillis :: Integer
                               , cpuTimeMillis :: Integer
                               , wallTimeMillis :: Integer
                               , processedRows :: Integer
                               , processedBytes :: Integer
    } deriving (Show, Generic)

data PrestoColumn = PrestoColumn {
      name :: String,
      _type :: String
    } deriving (Show, Generic)



data AnyValue = NumValue Scientific
              | TextValue T.Text
              | BoolValue Bool

instance Show AnyValue where
  show (NumValue n) = show n
  show (TextValue t) = show t
  show (BoolValue b) = show b

instance FromJSON AnyValue where
  parseJSON (Number n) = return $ NumValue n
  parseJSON (String t) = return $ TextValue t
  parseJSON (Bool b)   = return $ BoolValue b
  parseJSON _          = mzero

instance ToJSON AnyValue where
  toJSON (NumValue n) = Number n
  toJSON (TextValue t) = String t
  toJSON (BoolValue b) = Bool b


type PrestoRow = [SqlValue]

data PrestoResponse = PrestoResponse { id :: String
                                     , infoUri :: String
                                     , nextUri :: String
                                     , stats :: PrestoStats

                                     , columns :: Maybe [PrestoColumn]
                                     , _data :: Maybe [[AnyValue]]

    } deriving (Show, Generic)


$(deriveJSON jsonOptions ''PrestoStats)
$(deriveJSON jsonOptions ''PrestoResponse)
$(deriveJSON jsonOptions ''PrestoColumn)

-- instance FromJSON PrestoStats
-- instance ToJSON PrestoStats

-- instance FromJSON PrestoResponse
-- instance ToJSON PrestoResponse

-- instance FromJSON PrestoColumn
-- instance ToJSON PrestoColumn
