{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
module Database.HDBC.Presto where

-- import Network.HTTP (simpleHTTP, getResponseBody)
-- import Network.HTTP.Base
-- import Network.HTTP.Headers
import Network.URI

import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString as BN

import Data.Convertible
import Data.Maybe
import GHC.Word (Word16)

import Control.Monad.Trans
import Control.Monad.Trans.Either

import Database.HDBC.PrestoData
import Data.Aeson

import Control.Concurrent (threadDelay)

import Safe (readMay)

import Control.Applicative ((<$>))

-- TODO: remove
import System.IO.Error
import Data.Either.Unwrap

import Data.Text.Lazy as T
import Data.Text.Lazy.Encoding (encodeUtf8)
import qualified Data.Text.Encoding as DE

-- HDBC stuff
import Database.HDBC (IConnection(..), SqlValue, fetchAllRows)
import Database.HDBC.Statement (Statement(..))
import Database.HDBC.SqlValue
import Database.HDBC.ColTypes as CT

import qualified System.IO.Streams as Streams
import Network.Http.Client

import Control.Concurrent.MVar(MVar, readMVar, modifyMVar, modifyMVar_, newMVar, withMVar, isEmptyMVar)

-- TODO: move to utility library or find in real library
maybeToEither = flip maybe Right . Left
maybeToEitherT x y = hoistEither $ maybeToEither x y

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
      _ <- execute showTables []
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
data PrestoConnection = PrestoConnection URI Connection

data PrestoStatement = PrestoStatement StmtState PrestoConnection Text

connectToPresto :: URI -> IO (Either Text PrestoConnection)
connectToPresto uri = runEitherT $ do
  auth <- maybeToEitherT "URI didn't contain authority" $ uriAuthority uri
  port <- maybeToEitherT "Couldn't read port from URI" $ readMay $ Prelude.tail $ uriPort auth

  let uriStr = DE.encodeUtf8 $ convert $ uriToString id uri ("" :: String)

  eitherConn <- liftIO $ tryIOError $ openConnection "localhost" port

  conn <- hoistEither $ mapLeft (convert . show) eitherConn

  return $ PrestoConnection uri conn


fetchPrestoRow :: PrestoConnection -> MVar PrestoStatement -> IO (Maybe [SqlValue])
fetchPrestoRow conn prestoStatementVar = modifyMVar prestoStatementVar fetch where
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
    exec (PrestoStatement Prepared (PrestoConnection uri httpConn) query) = do
      result <- doPollingPrestoQuery httpConn uri query
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

------------------- Connection and polling stuff -------------------

getPrestoResponse :: Connection -> URI -> Method -> B.ByteString -> EitherT String IO BN.ByteString
getPrestoResponse conn uri method body = do
  request <- liftIO $ buildRequest $ do
    http method (DE.encodeUtf8 $ convert $ uriPath uri)
    setContentType "text/plain"
    setAccept "*/*"
    setHeader "X-Presto-User" "presto"
    setHeader "X-Presto-Catalog" "tpch"
    setHeader "X-Presto-Schema" "tiny"

  bodyStream <- liftIO $ Streams.fromByteString $ convert body
  liftIO $ sendRequest conn request $ inputStreamBody bodyStream

  maybeBytes <- liftIO $ receiveResponse conn (\p i -> Streams.read i)

  maybeToEitherT "Failed to get bytes" maybeBytes


pollForResult :: Connection -> URI -> Int -> EitherT String IO ([PrestoColumn], [[SqlValue]])
pollForResult conn uri attemptNo = do
  liftIO $ threadDelay (attemptNo * 100000) -- Increase backoff in increments of 100ms = 100000 us
  body <- getPrestoResponse conn uri GET B.empty

  response <- maybeToEitherT ("Parse error of polling Presto response: " ++ show body) (decode $ convert body)

  case _data response of
    Nothing -> do
      newUri <- case parseAbsoluteURI $ nextUri response of
               Nothing -> left "Failed to parse nextUri"
               Just newUri -> right newUri
      pollForResult conn newUri (attemptNo + 1)

    Just dat -> case columns response of
                  Nothing -> left "Data but no columns found in response"
                  Just cols -> right (cols, fmap (makeSqlValues cols) dat)


initialUri = fromJust $ parseRelativeReference "/v1/statement"

doPollingPrestoQuery :: Connection -> URI -> T.Text -> IO (Either String ([PrestoColumn], [PrestoRow]))
doPollingPrestoQuery conn _ q = runEitherT $ do
  body <- getPrestoResponse conn initialUri POST (encodeUtf8 q)

  initialResponse <- maybeToEitherT ("Parse error of initial Presto response: " ++ show body) $ decode (convert body)

  newUri <- case parseAbsoluteURI $ nextUri initialResponse of
             Nothing -> left "Failed to parse nextUri"
             Just newUri -> right newUri

  pollForResult conn newUri 0


------------------- SQL stuff -------------------


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


makeSqlValue :: PrestoColumn -> AnyValue -> SqlValue
makeSqlValue column value = case (prestoColumn_type column, value) of
                             ("varchar", TextValue t) -> SqlString $ T.unpack $ T.fromStrict t
                             ("double", NumValue s) -> SqlDouble (realToFrac s)
                             ("bigint", NumValue s) -> SqlInteger $ floor $ realToFrac s
                             ("boolean", BoolValue b) -> SqlBool b
                             _ -> error $ "Unrecognized Presto column type: " ++ (prestoColumn_type column)

makeSqlValues :: [PrestoColumn] -> [AnyValue] -> [SqlValue]
makeSqlValues columns strings = [makeSqlValue column string | (column, string) <- Prelude.zip columns strings]
