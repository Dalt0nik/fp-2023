{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}

{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-deprecations #-}
{-# LANGUAGE ImportQualifiedPost #-}



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
import Data.Aeson hiding (Value) 
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.List
import Data.Char
import Data.ByteString hiding (zipWith, map, isPrefixOf, filter)
import Lib2 (ParsedStatement(InsertStatement))
import Data.Data (Data)
import GHC.Generics (Generic)
import Control.Monad.Trans.Error (throwError)
import Control.Monad
import Data.Either (isRight)



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
executeInsert (Lib2.InsertStatement tableName insertColumns insertValues) = do --insertColumns is a string
  -- Get the existing DataFrame
  loadedDF <- loadFile tableName
  let existingColumns = dataframeColumns loadedDF
--  let checkColumnsExist = Data.List.all (\colName -> Column colName StringType `Data.List.elem` existingColumns) insertColumns
--validate if provided column names are the same as column names in a dataframe
  let allColumnsExistInDataFrame = Data.List.all (`Data.List.elem` map (\(Column name _) -> name) existingColumns) insertColumns

  if allColumnsExistInDataFrame
    then do
      -- Validate if the values match the types of columns
      let expectedTypes = getColumnTypes existingColumns --returns list of column types
      let valuesAreValid = validateValues expectedTypes insertValues -- 
      if valuesAreValid
        then do
          -- If validation passes, add the new row to the DataFrame
          let newRows = dataframeRows loadedDF ++ insertValues
          let newDF = (DataFrame existingColumns newRows)
          saveTable tableName newDF
          return $ Right newDF
        else return $ Left $ "Invalid values for columns"
    else return $ Left "Columns do not exist in the DataFrame"

dataframeRows :: Either ErrorMessage DataFrame -> [Row]
dataframeRows (Right (DataFrame _ rows)) = rows
dataframeRows (Left _) = []  -- todo

dataframeColumns :: Either ErrorMessage DataFrame -> [Column]
dataframeColumns (Right (DataFrame columns _)) = columns
dataframeColumns (Left _) = []  -- todo


-- Function to get the column types for a list of column names
getColumnTypes :: [Column] -> [ColumnType]
getColumnTypes columns = map (\(Column _ colType) -> colType) columns


-- Function to validate if values match the expected types
validateValues :: [ColumnType] -> [[Value]] -> Bool
validateValues columnTypes values =
  Data.List.all (\entry -> Data.List.length entry == Data.List.length columnTypes && Data.List.all id (zipWith validateValue columnTypes entry)) values

-- Function to validate a single value against its expected type
validateValue :: ColumnType -> Value -> Bool
validateValue IntegerType (IntegerValue _) = True
validateValue StringType (StringValue _) = True
validateValue BoolType (BoolValue _) = True
validateValue _ NullValue = True
validateValue _ _ = False

--executeUpdate :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)
--  UpdateStatement TableName [(ColumnName, Value)] (Maybe Condition)
--executeInsert (Lib2.InsertStatement tableName insertColumns insertValues) = do
executeUpdate :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeUpdate (Lib2.UpdateStatement tableName updates condition) = do
  -- Get the existing DataFrame
  loadedDF <- loadFile tableName
  let existingColumns = dataframeColumns loadedDF

  -- Check if columns to update exist in the DataFrame
  let updateColumnNames = map fst updates
  let allColumnsExistInDataFrame = Data.List.all (`Data.List.elem` map (\(Column name _) -> name) existingColumns) updateColumnNames

  if allColumnsExistInDataFrame
    then do
      -- Validate if values to update are of proper type
      let updatedValues = map snd updates
      let expectedTypes = getColumnTypes existingColumns
      let valuesAreValid = validateValues expectedTypes [updatedValues]

      if True --valuesAreValid
        then do
          -- Filter rows based on the condition
          let maybeDataFrame = extractDataFrame loadedDF
          let filteredRows = case maybeDataFrame of
                Just df -> Lib2.filterRows existingColumns df condition --returns [Row]
                Nothing -> []  -- Handle the error case appropriately
          -- Update the DataFrame with new values
          let updatedRows = updateRows existingColumns updateColumnNames updates filteredRows
          let newDF = DataFrame existingColumns updatedRows

          saveTable tableName newDF
          return $ Right newDF
        else return $ Left $ "Invalid values for columns"
    else return $ Left "Columns to update do not exist in the DataFrame"

extractDataFrame :: Either ErrorMessage DataFrame -> Maybe DataFrame
extractDataFrame (Right df) = Just df
extractDataFrame _ = Nothing

-- Function to update a single row based on the provided updates
updateRows :: [Column] -> [ColumnName] -> [(ColumnName, Value)] -> [Row] -> [Row]
updateRows columns columnNames updates rows =
  map (\row -> updateValues columns columnNames updates row) rows
  where
    updateValues :: [Column] -> [ColumnName] -> [(ColumnName, Value)] -> Row -> Row
    updateValues columns colNames updates' row =
      map (\(colName, colValue) -> if colName `Data.List.elem` colNames then colValue else getOriginalValue columns colName row updates') updates'

    getOriginalValue :: [Column] -> ColumnName -> Row -> [(ColumnName, Value)] -> Value
    getOriginalValue columns colName row' updates' =
      case lookup colName updates' of
        Just updatedValue -> updatedValue
        Nothing -> getColumnValue columns colName row'

    getColumnValue :: [Column] -> ColumnName -> Row -> Value
    getColumnValue columns colName row' =
      case Lib2.findColumnIndex columns colName of
        Just colIndex -> row' !! colIndex
        Nothing -> error "Column not found in dataframe"  -- Handle the error case appropriately
        
-- Function to replace old rows with updated rows
removeDuplicates :: Eq a => [a] -> [a]
removeDuplicates [] = []
removeDuplicates (x:xs) = x : removeDuplicates (filter (/= x) xs)

replaceRows :: Either ErrorMessage DataFrame -> [Row] -> [Row] -> [Row]
replaceRows (Right (DataFrame columns _)) oldRows newRows =
  let remainingRows = removeDuplicates (oldRows ++ newRows)
  in remainingRows
replaceRows _ oldRows _ = oldRows




--Right (InsertStatement "employees" ["col1","col2"] [[StringValue "abc",IntegerValue 1],[StringValue "def",NullValue]])

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
    _ | "update" `isPrefixOf` sql' -> do
        parsedStatement <- parseStatement sql
        executeUpdate parsedStatement
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

