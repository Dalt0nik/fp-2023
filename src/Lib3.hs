{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}
module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    showTable
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import Data.Time ( UTCTime )
import InMemoryTables (database, TableName, tableEmployees)
import Lib2 qualified
import Data.Aeson 
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.List (intercalate)
import Data.Char


type FileContent = String
type ErrorMessage = String
type Database = [(TableName, DataFrame)]
type ColumnName = String
type Execution = Free ExecutionAlgebra

data AggregateFunction
  = Min
  | Sum deriving (Show, Eq)

data Operator = Equals
              | NotEquals
              | LessThanOrEqual
              | GreaterThanOrEqual deriving (Show, Eq)

data LogicalOp = Or deriving (Show, Eq)

data WhereAtomicStatement = Where ColumnName Operator String deriving (Show, Eq)

data Condition
  = Comparison WhereAtomicStatement [(LogicalOp, WhereAtomicStatement)] deriving (Show, Eq) -- string aka column
  

data Columns = All
  | SelectedColumns [String] 
  | Aggregation [(AggregateFunction, String)] deriving (Show, Eq) -- string aka column

data ParsedStatement
  = ShowTablesStatement
  | ShowTableStatement TableName
  | SelectStatement Columns TableName (Maybe Condition) deriving (Show, Eq)-- Condition

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next) -- deserialize table
  | SaveTable (TableName, DataFrame) (()-> next) --serialize table
  | GetCurrentTime (UTCTime -> next)
  | ShowTable TableName (Either ErrorMessage DataFrame -> next)
  | ParseStatement String (Lib2.ParsedStatement -> next)
  | ExecuteStatement Lib2.ParsedStatement (Either ErrorMessage DataFrame -> next)
  deriving Functor


loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id --why id?

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

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql =
  case sql of
    "now()" -> do
        currentTime <- getCurrentTime
        let column = Column "time" StringType
            value = StringValue (show currentTime)
        return $ Right $ DataFrame [column] [[value]]
    _ -> do --Ошибки еще не работают, вылетает
      parsedStatement <- parseStatement sql
      executeStatement parsedStatement


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

save :: DataFrame -> TableName -> IO ()
save df tableName = do
  let filePath = "db/" ++ tableName ++ ".json"
  let jsonStr = serializeDataFrame df
  writeFile filePath jsonStr

load :: TableName -> IO DataFrame
load tableName = do
  let filePath = "db/" ++ tableName ++ ".json"
  jsonStr <- readFile filePath
  case eitherDecode (BSLC.pack jsonStr) of --decode (eitherDecode in our case) takes a ByteStream as an arguments, that's why we need to convert jsonStr into byteStream
    Right df -> return df
    Left err -> error $ "Failed to decode JSON: " ++ err

main :: IO ()
main = do
  let (tableName, df) = tableEmployees
  save df tableName --save to json

  loadedDataFrame <- load tableName -- Load from JSON
  print loadedDataFrame

