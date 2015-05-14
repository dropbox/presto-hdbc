{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
module Database.HDBC.Presto (PrestoConnection(..), connectToPresto) where

import Control.Applicative ((<$>))
import Control.Concurrent (threadDelay)
import Control.Exception (throw)
import Control.Monad (when)
import Control.Monad.Trans
import Control.Monad.Trans.Either
import Data.Aeson
import qualified Data.ByteString as BN
import qualified Data.ByteString.Lazy as B
import Data.Convertible
import Data.Maybe
import Database.HDBC.PrestoData
import GHC.Word (Word16)
import Network.URI
import Safe (readMay)

-- TODO: remove
import Data.Either.Unwrap
import qualified Data.Text.Encoding as DE
import qualified Data.Text.Lazy.Encoding as LDE
import Data.Text.Lazy as T
import Data.Text.Lazy.Encoding (encodeUtf8)
import System.IO.Error

-- HDBC stuff
import Control.Concurrent.MVar(MVar, readMVar, modifyMVar, modifyMVar_, newMVar, withMVar, isEmptyMVar)
import Database.HDBC (IConnection(..), SqlValue, fetchAllRows, SqlError(..))
import Database.HDBC.ColTypes as CT
import Database.HDBC.SqlValue
import Database.HDBC.Statement (Statement(..))
import Network.Http.Client
import qualified System.IO.Streams as Streams

import Util

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
                                                colSize = Just $ if partition then 1 else 0, -- Hide the partition info here
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

type Catalog = T.Text
type Schema = T.Text

-- Store the uri in here for now
data PrestoConnection = PrestoConnection URI Connection Catalog Schema

data PrestoStatement = PrestoStatement StmtState PrestoConnection Text

connectToPresto :: URI -> T.Text -> T.Text -> IO (Either Text PrestoConnection)
connectToPresto uri catalog schema = runEitherT $ do
  auth <- maybeToEitherT "URI didn't contain authority" $ uriAuthority uri
  port <- maybeToEitherT "Couldn't read port from URI" $ readMay $ Prelude.tail $ uriPort auth

  eitherConn <- liftIO $ tryIOError $ openConnection "localhost" port
  conn <- hoistEither $ mapLeft (convert . show) eitherConn
  return $ PrestoConnection uri conn catalog schema


fetchPrestoRow :: PrestoConnection -> MVar PrestoStatement -> IO (Maybe [SqlValue])
fetchPrestoRow conn prestoStatementVar = modifyMVar prestoStatementVar fetch where
    fetch (PrestoStatement Prepared _ _) = fail "Trying to fetch before executing statement."

    fetch statement@(PrestoStatement (Executed _ []) conn query) = do
      let newStatement = PrestoStatement Finished conn query
      return (newStatement, Nothing)

    fetch statement@(PrestoStatement (Executed cols rows@(row:remainder)) conn query) = do
      let newStatement = PrestoStatement (Executed cols remainder) conn query
      return (newStatement, Just row)

    fetch (PrestoStatement (Errored msg) _ _) = throw $ SqlError "Query failed" 0 msg

    fetch (PrestoStatement Finished _ _ ) = throw $ SqlError "Can't execute an already executed statement." 0 ""

executePresto conn prestoStatementVar vals = modifyMVar prestoStatementVar exec where
    exec stmt@(PrestoStatement (Executed _ _) _ _) = return (stmt, 0)
    exec (PrestoStatement Prepared (PrestoConnection uri httpConn _ _) query) = do
      result <- doPollingPrestoQuery httpConn conn uri query
      let newStatement = case result of
                          Left msg -> throw $ SqlError "Query failed" 0 msg
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

        return $ [(prestoColumn_name x, SqlColDesc {
                     colType = prestoTypeToSqlColType $ prestoColumn_type x,
                     colSize = Nothing,
                     colOctetLength = Nothing,
                     colDecDigits = Nothing,
                     colNullable = Nothing
                     }) | x <- columns]
    }

disconnectPresto :: PrestoConnection -> IO ()
disconnectPresto conn = return ()
-- TODO: the JDBC version seems to do a DELETE when you close the StatementClient

------------------- Connection and polling stuff -------------------

getPrestoResponse :: (FromJSON a, ToJSON a) => Connection -> PrestoConnection -> URI -> Method -> B.ByteString -> EitherT String IO a
getPrestoResponse conn (PrestoConnection _ _ catalog schema) uri method body = do
  request <- liftIO $ buildRequest $ do
    http method (DE.encodeUtf8 $ convert $ uriPath uri)
    setContentType "text/plain"
    setAccept "*/*"
    setHeader "X-Presto-User" "presto"
    setHeader "X-Presto-Catalog" (DE.encodeUtf8 $ convert catalog)
    setHeader "X-Presto-Schema" (DE.encodeUtf8 $ convert schema)

  bodyStream <- liftIO $ Streams.fromByteString $ convert body
  liftIO $ sendRequest conn request $ inputStreamBody bodyStream

  -- maybeBytes <- liftIO $ receiveResponse conn (\_ i -> do
  --                                                 (i2, lenio) <- Streams.countInput i
  --                                                 len <- lenio
  --                                                 putStrLn $ "Bytes: " ++ show len
  --                                                 Streams.read i)

  liftIO $ receiveResponse conn jsonHandler


pollForResult :: Connection -> PrestoConnection -> URI -> Int -> EitherT String IO ([PrestoColumn], [[SqlValue]])
pollForResult conn prestoConn uri attemptNo = do
  liftIO $ threadDelay (attemptNo * 100000) -- Linear backoff in increments of 100ms = 100000 us
  response <- getPrestoResponse conn prestoConn uri GET B.empty

  case _data response of
    Nothing -> do
      when (isNothing $ nextUri response) $ do
        iUri <- hoistEither $ maybeToEither "No data or nextUri and couldn't parse infoUri" $ parseAbsoluteURI (infoUri response)
        info :: PrestoInfo <- getPrestoResponse conn prestoConn iUri GET B.empty
        left $ "No data and no nextUri: " ++ show response ++ "\n\n" ++ "Info: " ++ show info
      newUri <- case parseAbsoluteURI $ fromJust $ nextUri response of
               Nothing -> left "Failed to parse nextUri"
               Just newUri -> right newUri
      pollForResult conn prestoConn newUri (attemptNo + 1)

    Just dat -> case columns response of
                  Nothing -> left "Data but no columns found in response"
                  Just cols -> right (cols, fmap (makeSqlValues cols) dat)


initialUri = fromJust $ parseRelativeReference "/v1/statement"

doPollingPrestoQuery :: Connection -> PrestoConnection -> URI -> T.Text -> IO (Either String ([PrestoColumn], [PrestoRow]))
doPollingPrestoQuery conn prestoConn _ q = runEitherT $ do
  initialResponse <- getPrestoResponse conn prestoConn initialUri POST (encodeUtf8 q)

  uri <- maybeToEitherT "No nextUri found in initialResponse" $ nextUri initialResponse

  newUri <- case parseAbsoluteURI uri of
             Nothing -> left "Failed to parse nextUri"
             Just newUri -> right newUri

  pollForResult conn prestoConn newUri 0


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
                             (_, NullValue) -> SqlNull
                             _ -> error $ "Unrecognized Presto column type: " ++ (prestoColumn_type column)

makeSqlValues :: [PrestoColumn] -> [AnyValue] -> [SqlValue]
makeSqlValues columns strings = [makeSqlValue column string | (column, string) <- Prelude.zip columns strings]
