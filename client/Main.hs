{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
module Main (main) where

import Network.Wreq
import qualified Data.Text as T
import Data.Aeson (ToJSON, FromJSON, toJSON, parseJSON, withObject, (.:))
import GHC.Generics (Generic)
import Control.Lens
import Data.Text.Encoding (decodeUtf8)
import Data.ByteString.Lazy (ByteString)
import Data.ByteString (fromStrict)
import Data.ByteString.Lazy (ByteString)
import Data.ByteString (toStrict)

-- Define a function to get the table from the server
getTableFromServer :: String -> IO (Maybe ByteString)
getTableFromServer tableName = do
  let url = "http://localhost:3000/tables/" ++ tableName
  response <- get url
  let body = response ^. responseBody
  return $ if response ^. responseStatus . statusCode == 404
    then Nothing
    else Just body

main :: IO ()
main = do
  -- Replace "your_table_name" with the actual table name you want to fetch
  let tableName = "flags"
  result <- getTableFromServer tableName
  case result of
    Just table -> putStrLn $ "Received table:\n" ++  T.unpack (decodeUtf8 $ toStrict table)
    Nothing    -> putStrLn $ "Table not found."
