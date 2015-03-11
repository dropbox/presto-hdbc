{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE TemplateHaskell #-}
module Database.HDBC.PrestoData where

import Control.Monad
import Data.Aeson
import Data.Aeson.TH
import Data.Scientific
import Data.Text as T
import Database.HDBC (SqlValue)
import GHC.Generics
import Util.JSONConventions

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


data AnyValue = NumValue Scientific
              | TextValue T.Text
              | BoolValue Bool
              | NullValue

instance Show AnyValue where
  show (NumValue n) = show n
  show (TextValue t) = show t
  show (BoolValue b) = show b

instance FromJSON AnyValue where
  parseJSON (Number n) = return $ NumValue n
  parseJSON (String t) = return $ TextValue t
  parseJSON (Bool b)   = return $ BoolValue b
  parseJSON (Array a) = error $ "Don't know how to parse array: " ++ show a
  parseJSON (Object o) = error $ "Don't know how to parse object: " ++ show o
  parseJSON Null = return $ NullValue

instance ToJSON AnyValue where
  toJSON (NumValue n) = Number n
  toJSON (TextValue t) = String t
  toJSON (BoolValue b) = Bool b


type PrestoRow = [SqlValue]
data PrestoColumn = PrestoColumn {
      prestoColumn_name :: String,
      prestoColumn_type :: String
    } deriving (Show, Generic)

data PrestoResponse = PrestoResponse { response_id :: String
                                     , infoUri :: String
                                     , nextUri :: Maybe String
                                     , stats :: PrestoStats

                                     , columns :: Maybe [PrestoColumn]
                                     , _data :: Maybe [[AnyValue]]

    } deriving (Show, Generic)

data PrestoInfo = PrestoInfo { pi_queryId :: String
                             , pi_session :: PrestoSession
                             , pi_state :: String
                             , pi_self :: String
                             , pi_fieldNames :: [String]
                             , pi_query :: String
                             , pi_queryStats :: PrestoQueryStats
                             , pi_failureInfo :: Maybe PrestoFailureInfo
                             , pi_errorCode :: Maybe PrestoErrorCode
                             , pi_inputs :: [String]
                             } deriving (Show, Generic)


data PrestoSession = PrestoSession { ps_user :: String
                                   , ps_catalog :: String
                                   , ps_schema :: String
                                   , ps_timeZoneKey :: Int
                                   , ps_locale :: String
                                   , ps_remoteUserAddress :: String
                                   , ps_startTime :: Int
                                   } deriving (Show, Generic)

data PrestoQueryStats = PrestoQueryStats { pqs_createTime :: String
                                         , pqs_lastHeartbeat :: String
                                         , pqs_endTime :: String
                                         , pqs_elapsedTime :: String
                                         , pqs_queuedTime :: String
                                         , pqs_totalTasks :: Int
                                         , pqs_runningTasks :: Int
                                         , pqs_completedTasks :: Int
                                         , pqs_totalDrivers :: Int
                                         , pqs_queuedDrivers :: Int
                                         , pqs_runningDrivers :: Int
                                         , pqs_completedDrivers :: Int
                                         , pqs_totalMemoryReservation :: String
                                         , pqs_totalScheduledTime :: String
                                         , pqs_totalCpuTime :: String
                                         , pqs_totalUserTime :: String
                                         , pqs_totalBlockedTime :: String
                                         , pqs_rawInputDataSize :: String
                                         , pqs_rawInputPositions :: Int
                                         , pqs_processedInputDataSize :: String
                                         , pqs_processedInputPositions :: Int
                                         , pqs_outputDataSize :: String
                                         , pqs_outputPositions :: Int
                                         } deriving (Show, Generic)

data PrestoFailureInfo = PrestoFailureInfo { pfi_type :: String
                                           , pfi_message :: String
                                           , pfi_suppressed :: [String]
                                           , pfi_stack :: [String]
                                           } deriving (Show, Generic)

data PrestoErrorCode = PrestoErrorCode { pec_code :: Int
                                       , pec_name :: String
                                       } deriving (Show, Generic)

$(deriveJSON jsonOptions ''PrestoStats)
$(deriveJSON jsonOptions ''PrestoResponse)
$(deriveJSON jsonOptions ''PrestoColumn)
$(deriveJSON jsonOptions ''PrestoQueryStats)
$(deriveJSON jsonOptions ''PrestoSession)
$(deriveJSON jsonOptions ''PrestoFailureInfo)
$(deriveJSON jsonOptions ''PrestoErrorCode)
$(deriveJSON jsonOptions ''PrestoInfo)
