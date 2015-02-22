{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
module Database.HDBC.Presto where

import Network.HTTP
import Network.HTTP.Base
import Network.HTTP.Headers
import Network.URI

import Network.Stream

import Data.Maybe

import qualified Data.ByteString.Lazy as B

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Trans.Class
import Control.Monad.Trans.Either
import Control.Error.Util

import Database.HDBC.PrestoData
import Data.Aeson

import Control.Concurrent (threadDelay)

-- TODO: remove
import System.IO.Unsafe (unsafePerformIO)
import Debug.Trace
import Data.Either.Unwrap

import Data.Text.Lazy as T
import Data.Text.Lazy.Encoding (encodeUtf8)
-- import qualified Data.ByteString.Lazy.Char8 as BC

-- HDBC stuff
import Database.HDBC (IConnection(..), SqlValue, fetchAllRows)
import Database.HDBC.Statement (Statement(..))
import Database.HDBC.SqlValue
import Database.HDBC.ColTypes as CT

import Control.Concurrent.MVar(MVar, readMVar, modifyMVar, modifyMVar_, newMVar, withMVar, isEmptyMVar)

instance IConnection PrestoConnection where
    disconnect = disconnectPresto
    commit _ = fail "Not implemented"
    rollback _ = fail "Not implemented"
    run _ _ _ = fail "Not implemented"
    prepare = preparePresto
    clone _ = fail "Not implemented"
    hdbcDriverName _ = "presto"
    hdbcClientVer _ = fail "Not implemented"
    proxiedClientName _ = fail "Not implemented"
    proxiedClientVer _ = fail "Not implemented"
    dbServerVer _ = fail "Not implemented"
    dbTransactionSupport _ = False
    getTables conn = do
      showTables <- prepare conn "show tables"
      execute showTables []
      rows <- fetchAllRows showTables
      -- Each row should be a single string
      return $ fmap ((\(SqlString s) -> s) . Prelude.head) rows
    describeTable conn name = do
      statement <- prepare conn $ "describe " ++ name
      execute statement []
      rows <- fetchAllRows statement

      -- Presto's describe table output is of the following form:
      -- Column (varchar)
      -- Type (varchar)
      -- Null (boolean)
      -- Partition Key (boolean)
      -- Comment (boolean)

      -- Pack the data into a SqlColDesc
      return $ (flip fmap) rows (\((SqlString name):
                                   (SqlString typ):
                                   (SqlBool nullable):
                                   (SqlBool partition):
                                   (SqlString comment):
                                   xs) ->
                                     (name, SqlColDesc {
                                                colType = prestoTypeToSqlColType typ,
                                                colSize = Just $ if partition then 1 else 0,
                                                colOctetLength = Nothing,
                                                colDecDigits = Nothing,
                                                colNullable = Just nullable
                                              }))


data StmtState = Prepared
               | Executed [PrestoColumn] [PrestoRow]
               | Errored String
               | Finished

columnsFromStatement :: PrestoStatement -> [PrestoColumn]
columnsFromStatement statement = case statement of
                                   PrestoStatement (Executed cols rows) _ _ -> cols
                                   _ -> fail "You have to call execute before getting column names"

-- Store the uri in here for now
data PrestoConnection = PrestoConnection URI (HandleStream String)

data PrestoStatement = PrestoStatement StmtState PrestoConnection Text

connectToPresto :: URI -> IO PrestoConnection
connectToPresto uri = do
  -- stream <- openTCPConnection (uriToString Prelude.id uri "") 5096
  return $ PrestoConnection uri undefined

finishPresto = undefined

fetchPrestoRow :: PrestoConnection -> MVar PrestoStatement -> IO (Maybe [SqlValue])
fetchPrestoRow conn prestoStatementVar = do
  modifyMVar prestoStatementVar fetch where
    fetch (PrestoStatement Prepared _ _) = fail "Trying to fetch before executing statement."

    fetch statement@(PrestoStatement (Executed _ []) conn query) = do
      let newStatement = PrestoStatement Finished conn query
      return (newStatement, Nothing)

    fetch statement@(PrestoStatement (Executed cols rows@(row:remainder)) conn query) = do
      let newStatement = PrestoStatement (Executed cols remainder) conn query
      return (newStatement, Just row)

    fetch (PrestoStatement (Errored msg) _ _) = fail $ "Query failed: " ++ msg -- TODO: throw SqlError

    fetch (PrestoStatement Finished _ _ ) = fail "Can't execute an already finished statement."


executePresto conn prestoStatementVar vals = modifyMVar prestoStatementVar exec where
    exec stmt@(PrestoStatement (Executed _ _) _ _) = return (stmt, 0)
    exec (PrestoStatement Prepared (PrestoConnection uri _) query) = do
      result <- doPollingPrestoQuery uri query
      let newStatement = case result of
                          Left msg -> PrestoStatement (Errored msg) conn query -- TODO: throw SqlError? (check if this should happen on HDBC execute)
                          Right (columns, rows) -> PrestoStatement (Executed columns rows) conn query
      return (newStatement, 0)


preparePresto :: PrestoConnection -> String -> IO Statement
preparePresto conn query = do
  prestoStatementVar <- newMVar $ PrestoStatement Prepared conn (T.pack query)

  return Statement {
    executeRaw = undefined
    , execute = executePresto conn prestoStatementVar
    , executeMany = \_ -> fail "Not implemented"
    , finish = fail "Not implemented"
    , fetchRow = fetchPrestoRow conn prestoStatementVar
    , originalQuery = query
    , getColumnNames = do
        prestoStatement <- readMVar prestoStatementVar
        let columns = columnsFromStatement prestoStatement
        return $ fmap prestoColumn_name columns
    , describeResult = do
        prestoStatement <- readMVar prestoStatementVar
        let columns = columnsFromStatement prestoStatement

        putStrLn $ "Got columns result: " ++ show columns

        return $ (flip fmap) columns (\column -> (prestoColumn_name column, SqlColDesc {
                                                                          colType = prestoTypeToSqlColType $ prestoColumn_type column,
                                                                          colSize = Nothing,
                                                                          colOctetLength = Nothing,
                                                                          colDecDigits = Nothing,
                                                                          colNullable = Nothing
                                                                        }))
    }

disconnectPresto :: PrestoConnection -> IO ()
disconnectPresto conn = return ()
-- TODO: the JDBC version seems to do a DELETE when you close the StatementClient

---------------------------------------------------------------
------------------- Private, non-HDBC stuff -------------------
---------------------------------------------------------------

prestoTypeToSqlColType :: String -> CT.SqlTypeId
prestoTypeToSqlColType prestoType = case prestoType of
                                      "varchar" -> SqlVarCharT
                                      "bigint" -> SqlBigIntT
                                      "boolean" -> SqlBitT
                                      "double" -> SqlDoubleT
                                      "date" -> SqlDateT
                                      "time" -> SqlTimeT
                                      "time_with_timezone" -> SqlTimeWithZoneT
                                      "timestamp" -> SqlTimestampT
                                      "timestamp_with_timezone" -> SqlTimestampWithZoneT
                                      "interval_year_to_month" -> SqlIntervalT SqlIntervalYearToMonthT
                                      "interval_year_to_second" -> error "Can't represent year to second as SqlTypeId (?)"


buildPrestoRequest :: URI -> RequestMethod -> B.ByteString -> Request B.ByteString
buildPrestoRequest uri method s = Request uri method headers s where
    headers = [mkHeader HdrContentType "text/plain",
               mkHeader HdrAccept "*/*",
               mkHeader (HdrCustom "X-Presto-User") "presto",
               mkHeader (HdrCustom "X-Presto-Catalog") "tpch",
               mkHeader (HdrCustom "X-Presto-Schema") "tiny",
               mkHeader HdrContentLength $ show $ B.length s]


getPrestoResponseBody request = do result <- lift $ simpleHTTP request
                                   case result of
                                     Left ErrorClosed -> left "Network connection closed"
                                     Left ErrorReset -> left "Network connection reset"
                                     Left (ErrorParse s) -> left s
                                     Left (ErrorMisc s) -> left s
                                     Right r -> lift $ getResponseBody result

initialQuery :: URI -> T.Text -> IO (Either String PrestoResponse)
initialQuery uri q = runEitherT $ do
  body <- getPrestoResponseBody $ buildPrestoRequest uri POST $ encodeUtf8 q

  case (decode body) :: Maybe PrestoResponse of
   Nothing -> left $ "Parse error of initial Presto response" ++ (show body)
   Just r -> right r

makeSqlValue :: PrestoColumn -> AnyValue -> SqlValue
makeSqlValue column value = case (prestoColumn_type column, value) of
                             ("varchar", TextValue t) -> SqlString $ T.unpack $ T.fromStrict t
                             ("double", NumValue s) -> SqlDouble (realToFrac s)
                             ("bigint", NumValue s) -> SqlInteger $ floor $ realToFrac s
                             ("boolean", BoolValue b) -> SqlBool b
                             _ -> error $ "Unrecognized Presto column type: " ++ (prestoColumn_type column)

makeSqlValues :: [PrestoColumn] -> [AnyValue] -> [SqlValue]
makeSqlValues columns strings = [makeSqlValue column string | (column, string) <- Prelude.zip columns strings]

pollForResult :: URI -> Int -> EitherT String IO ([PrestoColumn], [[SqlValue]])
pollForResult uri attemptNo = do
  liftIO $ threadDelay (attemptNo * 100000) -- Increase backoff in increments of 100ms = 100000 us
  body <- getPrestoResponseBody $ buildPrestoRequest uri GET $ B.empty

  response <- case (decode body) :: Maybe PrestoResponse of
    Nothing -> left $ "Parse error of polling Presto response" ++ (show body)
    Just r -> right r

  case _data response of
    Nothing -> do
      uri <- case parseAbsoluteURI $ nextUri response of
               Nothing -> left "Failed to parse nextUri"
               Just uri -> right uri
      pollForResult uri (attemptNo + 1)

    Just dat -> case columns response of
                  Nothing -> left "Data but no columns found in response"
                  Just cols -> right (cols, fmap (makeSqlValues cols) dat)


doPollingPrestoQuery :: URI -> T.Text -> IO (Either String ([PrestoColumn], [PrestoRow]))
doPollingPrestoQuery uri q = runEitherT $ do
  initialResponse <- EitherT $ initialQuery uri q

  uri <- case parseAbsoluteURI $ nextUri initialResponse of
          Nothing -> left "Failed to parse nextUri"
          Just uri -> right uri

  pollForResult uri 0
