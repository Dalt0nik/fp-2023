{-# LANGUAGE OverloadedStrings #-}

import Web.Scotty
import qualified Data.Yaml as Yaml
import qualified Data.ByteString.Lazy as BS
import Data.Text (Text, pack, unpack)
import DataFrame-- (DataFrame, Column (..), ColumnType (..), Value (..), Row)
import InMemoryTables (database, TableName)
import Network.HTTP.Types (notFound404)
import Data.Aeson ( ToJSON, FromJSON )
import Data.ByteString (fromStrict)

instance FromJSON ColumnType
instance ToJSON ColumnType

instance FromJSON Column
instance ToJSON Column

instance FromJSON DataFrame.Value

instance ToJSON DataFrame.Value

instance FromJSON DataFrame
instance ToJSON DataFrame


-- Convert DataFrame to YAML ByteString
dataFrameToYaml :: DataFrame -> BS.ByteString
dataFrameToYaml df = fromStrict $ Yaml.encode df


getTableRoute :: TableName -> ActionM ()
getTableRoute tableName = do
  let maybeTable = lookup tableName database
  case maybeTable of
    Just table -> raw $ dataFrameToYaml table
    Nothing -> status notFound404

-- Define the main application
app :: ScottyM ()
app = do
  get "/tables/:name" $ do --http://localhost:3000/tables
    tableName <- param "name"
    getTableRoute tableName

-- Run the application on port 3000
main :: IO ()
main = scotty 3000 app