module Demo where

import Database.HDBC
import Database.HDBC.Presto

import Network.URI
import Data.Maybe

prestoUri = fromJust $ parseAbsoluteURI "http://localhost:5096/v1/statement"

main = do
  putStrLn "Testing out the HDBC Presto connector!"

  c <- connectToPresto prestoUri

  select <- prepare c "describe customer"

  execute select []

  -- fetchRow select

  fetchAllRows select
  -- disconnect c
