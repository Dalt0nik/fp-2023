{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-deprecations #-}
module Main (main) where

import Network.Wreq
import Data.Aeson (FromJSON)
import Control.Lens
import Data.ByteString.Lazy (ByteString)
import Data.ByteString ( fromStrict, toStrict )
import DataFrame
import qualified Data.Yaml as Yaml

instance FromJSON ColumnType
instance FromJSON Column
instance FromJSON Value
instance FromJSON DataFrame

yamlToDataFrame :: ByteString -> Maybe DataFrame
yamlToDataFrame bs = Yaml.decode (toStrict bs)

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
  let tableName = "flags"
  result <- getTableFromServer tableName
  case result of
    Just table -> do
      case yamlToDataFrame table of
        Just df -> putStrLn $ "Converted DataFrame:\n" ++ show df
        Nothing -> putStrLn "Failed to parse YAML data into DataFrame."
    Nothing    -> putStrLn "Table not found."

