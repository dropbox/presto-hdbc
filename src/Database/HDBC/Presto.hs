{-# LANGUAGE OverloadedStrings #-}
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

import Control.Monad.Trans.Class
import Control.Monad.Trans.Either
import Control.Error.Util

import Database.HDBC.PrestoData
import Data.Aeson

-- TODO: remove
import System.IO.Unsafe (unsafePerformIO)
import Debug.Trace
import Data.Either.Unwrap

import Data.Text.Lazy as T
import Data.Text.Lazy.Encoding (encodeUtf8)
-- import qualified Data.ByteString.Lazy.Char8 as BC

-- HDBC stuff
import Database.HDBC (IConnection(..), SqlValue)
import Database.HDBC.Statement (Statement(..))
import Database.HDBC.SqlValue

import Control.Concurrent.MVar(MVar, modifyMVar, modifyMVar_, newMVar, withMVar, isEmptyMVar)

type SqlQuery = T.Text
-- class IsString SqlQuery where
--     fromString :: fromString



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
    getTables _ = fail "Not implemented"
    describeTable _ _ = fail "Not implemented"

data StmtState = Prepared
               | Executed [PrestoRow]
               | Errored String
               | Finished


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
  result <- modifyMVar prestoStatementVar fetch
  -- if isNothing result then finishPresto prestoStatementVar else return result
  return result where

    fetch (PrestoStatement Prepared _ _) = fail "Trying to fetch before executing statement"

    fetch statement@(PrestoStatement (Executed []) conn query) = do
      let newStatement = PrestoStatement Finished conn query
      return (newStatement, Nothing)

    fetch statement@(PrestoStatement (Executed rows@(row:remainder)) conn query) = do
      let newStatement = PrestoStatement (Executed remainder) conn query
      return (newStatement, Just row)

    fetch (PrestoStatement (Errored msg) _ _) = fail $ "Query failed: " ++ msg

    fetch (PrestoStatement Finished _ _ ) = fail "Not implemented"



getPrestoColumnNames = undefined

-- Execute won't do anything for now
executePresto conn prestoStatementVar vals = modifyMVar prestoStatementVar exec where
    exec stmt@(PrestoStatement (Executed _) _ _) = return (stmt, 0)
    exec (PrestoStatement Prepared (PrestoConnection uri _) query) = do
      result <- doPollingPrestoQuery uri query
      let newStatement = case result of
                          Left msg -> PrestoStatement (Errored msg) conn query
                          Right rows -> PrestoStatement (Executed rows) conn query
      return (newStatement, 0)

-- Defining this even though it conflicts with the one in Database.HDBC.Utils
-- fetchAllRows :: Statement -> IO [[SqlValue]]
-- fetchAllRows (Statement {originalQuery}) =


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
    , getColumnNames = getPrestoColumnNames conn prestoStatementVar
    , describeResult = fail "Not implemented"
    }

disconnectPresto :: PrestoConnection -> IO ()
disconnectPresto conn = return ()


---------------------------------------------------------------
------------------- Private, non-HDBC stuff -------------------
---------------------------------------------------------------

-- hostUri = fromJust $ parseAbsoluteURI "http://localhost:5096/v1/statement"

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
makeSqlValue column value = case (_type column, value) of
                             ("varchar", TextValue t) -> SqlString $ T.unpack $ T.fromStrict t
                             ("double", NumValue s) -> SqlDouble (realToFrac s)
                             ("bigint", NumValue s) -> SqlInteger $ floor $ realToFrac s
                             _ -> error "Unrecognized Presto column type"

makeSqlValues :: [PrestoColumn] -> [AnyValue] -> [SqlValue]
makeSqlValues columns strings = [makeSqlValue column string | (column, string) <- Prelude.zip columns strings]

pollForResult uri = do
                      body <- getPrestoResponseBody $ buildPrestoRequest uri GET $ B.empty

                      -- traceM "Here's the body:"
                      -- traceM $ Prelude.map (toEnum . fromEnum) $ B.unpack body

                      response <- case (decode body) :: Maybe PrestoResponse of
                                    Nothing -> left $ "Parse error of polling Presto response" ++ (show body)
                                    Just r -> right r

                      -- traceM $ "Got response: " ++ (show response)

                      case _data response of
                        Nothing -> do
                          uri <- case parseAbsoluteURI $ nextUri response of
                                   Nothing -> left "Failed to parse nextUri"
                                   Just uri -> right uri
                          pollForResult uri

                        Just dat -> case columns response of
                                      Nothing -> left "Data but no columns found in response"
                                      Just cols -> right $ fmap (makeSqlValues cols) dat



doPollingPrestoQuery uri q = runEitherT $ do
  initialResponse <- EitherT $ initialQuery uri q

  uri <- case parseAbsoluteURI $ nextUri initialResponse of
          Nothing -> left "Failed to parse nextUri"
          Just uri -> right uri

  pollForResult uri
