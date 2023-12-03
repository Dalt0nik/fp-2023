{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}

{-# LANGUAGE DeriveGeneric #-}


module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    showTable,
    executeInsert,
    save,
    load,
    deserializeDataFrame,
    serializeDataFrame
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import Data.Time ( UTCTime )
import InMemoryTables (database, TableName, tableEmployees)
import Lib2 qualified
import Data.Aeson 
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.List
import Data.Char
import Data.ByteString hiding (map, isPrefixOf, filter)
import Lib2 (ParsedStatement(InsertStatement))
import Data.Data (Data)
import GHC.Generics (Generic)


type FileContent = String
type ErrorMessage = String
type Database = [(TableName, DataFrame)]
type ColumnName = String
type Execution = Free ExecutionAlgebra
type DeserializedTableNameDataFrame = (TableName, DataFrame)

 
data ExecutionAlgebra next
  = LoadFile TableName (Either ErrorMessage DataFrame -> next) -- deserialize table
  | SaveTable (TableName, DataFrame) (()-> next) --serialize table
  | GetCurrentTime (UTCTime -> next)
  | ShowTable TableName (Either ErrorMessage DataFrame -> next)
  | ParseStatement String (Lib2.ParsedStatement -> next)
  | ExecuteStatement Lib2.ParsedStatement (Either ErrorMessage DataFrame -> next)
  | ExecuteInsert Lib2.ParsedStatement (Either ErrorMessage DataFrame -> next)
  deriving Functor


loadFile :: TableName -> Execution (Either ErrorMessage DataFrame)
loadFile tableName = liftF $ LoadFile tableName id

saveTable :: TableName -> DataFrame -> Execution ()
saveTable tableName dataFrame = liftF $ SaveTable (tableName, dataFrame) id


getCurrentTime :: Execution UTCTime
getCurrentTime = liftF $ GetCurrentTime id

parseStatement :: String -> Execution Lib2.ParsedStatement
parseStatement input = liftF $ ParseStatement input id

executeStatement :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeStatement statement = liftF $ ExecuteStatement statement id

showTable :: TableName -> Execution (Either ErrorMessage DataFrame)
showTable tableName = case Lib2.fetchTableFromDatabase tableName of
  Right (_, table) -> return $ Right table
  Left errMsg -> return $ Left errMsg

executeInsert :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeInsert (Lib2.InsertStatement tableName columns values) = do
  loadedData <- loadFile tableName
  case loadedData of
    Left err -> return $ Left err
    Right dataFrame -> do
      saveTable "newTable" dataFrame
      return $ Right dataFrame
  

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
  let sql' = map toLower (filter (not . isSpace) sql)
  case sql' of
    "now()" -> do
        currentTime <- getCurrentTime
        let column = Column "time" StringType
            value = StringValue (show currentTime)
        return $ Right $ DataFrame [column] [[value]]
    _ | "select" `isPrefixOf` sql' -> do
        parsedStatement <- parseStatement sql
        executeStatement parsedStatement
    _ | "insert" `isPrefixOf` sql' -> do
        parsedStatement <- parseStatement sql
        executeInsert parsedStatement
    -- _ | "update" `isPrefixOf` sql' -> do
    --     parsedStatement <- parseStatement sql
    --     parsedStatement
    -- _ | "delete" `isPrefixOf` sql' -> do
    --     parsedStatement <- parseStatement sql
    --     parsedStatement
    _ -> do
      return $ Left "command not found"
      


---------SERIALISATION----DESERIALIZATION------------------

-- Custom Serialization Functions

serializeColumnType :: ColumnType -> String
serializeColumnType IntegerType = "\"IntegerType\""
serializeColumnType StringType  = "\"StringType\""
serializeColumnType BoolType    = "\"BoolType\""

serializeColumn :: Column -> String
serializeColumn (Column name colType) =
  "[\"" ++ name ++ "\", " ++ serializeColumnType colType ++ "]"

serializeValue :: DataFrame.Value -> String
serializeValue (IntegerValue intVal) = "{\"contents\":" ++ show intVal ++ ",\"tag\":\"IntegerValue\"}"
serializeValue (StringValue strVal)  = "{\"contents\":\"" ++ strVal ++ "\",\"tag\":\"StringValue\"}"
serializeValue (BoolValue boolVal)   = "{\"contents\":" ++ map toLower (show boolVal) ++ ",\"tag\":\"BoolValue\"}"
serializeValue NullValue              = "null"

serializeRow :: Row -> String
serializeRow row = "[" ++ Data.List.intercalate ", " (Prelude.map serializeValue row) ++ "]"

serializeDataFrame :: DataFrame -> String
serializeDataFrame (DataFrame columns rows) =
  "[[ " ++ Data.List.intercalate ", " (Prelude.map serializeColumn columns) ++ " ], " ++
  "[ " ++ Data.List.intercalate ", " (Prelude.map serializeRow rows) ++ " ]]"

-- Deserialization Instances
--instance ToJSON we nee if we want to use aeson encode function. so it's kinda our backup plan if something goes wrong
instance FromJSON ColumnType
--instance ToJSON ColumnType

instance FromJSON Column
--instance ToJSON Column

instance FromJSON DataFrame.Value
--instance ToJSON DataFrame.Value

instance FromJSON DataFrame
--instance ToJSON DataFrame

save :: DataFrame -> TableName -> IO()
save df tableName = do
  let filePath = "db/" ++ tableName ++ ".json"
  let jsonStr = serializeDataFrame df
  Prelude.writeFile filePath jsonStr

-- load :: TableName -> IO DataFrame
-- load tableName = do
--   let filePath = "db/" ++ tableName ++ ".json"
--   jsonStr <- Prelude.readFile filePath
--   case eitherDecode (BSLC.pack jsonStr) of --decode (eitherDecode in our case) takes a ByteStream as an arguments, that's why we need to convert jsonStr into byteStream
--     Right df -> return df
--     Left err -> error $ "Failed to decode JSON: " ++ err

load :: TableName -> IO FileContent
load tableName = do
  let filePath = "db/" ++ tableName ++ ".json"
  Prelude.readFile filePath    

deserializeDataFrame :: FileContent -> Either ErrorMessage DataFrame
deserializeDataFrame jsonStr =
  case eitherDecode (BSLC.pack jsonStr) of
    Right df -> Right df
    Left err -> Left $ "Failed to decode JSON: " ++ err

main :: IO()
main = do
  let (tableName, df) = tableEmployees
  save df tableName --save to json

  loadedDataFrame <- load tableName -- Load from JSON
  print loadedDataFrame

