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
    executeInsert, --i think we don't need to export this
    --save,
    --load,
    deserializeDataFrame,
    serializeDataFrame
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import Data.Time ( UTCTime )
import InMemoryTables (TableName, tableEmployees)
import Lib2 qualified
import Data.Aeson hiding (Value) 
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.List
import Data.Char
import Lib2 (ParsedStatement(InsertStatement))



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

--insert into employees (id, name, surname) values (69, 'a','b');
--InsertStatement TableName [ColumnName] [[Value]]
--InsertStatement "employees" ["id","name","surname"] [[IntegerValue 69,StringValue "a",StringValue "b"]]
executeInsert :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeInsert (Lib2.InsertStatement tableName insertColumns insertValues) = do --insertColumns is a string
  -- Get the existing DataFrame
  loadedDF <- loadFile tableName
  let existingColumns = dataframeColumns loadedDF
--validate if provided column names are the same as column names in a dataframe
  let allColumnsExistInDataFrame = Data.List.all (`Data.List.elem` map (\(Column name _) -> name) existingColumns) insertColumns

  if allColumnsExistInDataFrame
    then do
      -- Validate if the values match the types of columns
      let expectedTypes = getColumnTypes existingColumns --returns list of column types
      let valuesAreValid = validateValuesForInsert expectedTypes insertValues -- 
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
validateValuesForInsert :: [ColumnType] -> [[Value]] -> Bool
validateValuesForInsert columnTypes values =
  Data.List.all (\entry -> Data.List.length entry == Data.List.length columnTypes && Data.List.all id (zipWith validateValue columnTypes entry)) values

-- Function to validate a single value against its expected type
validateValue :: ColumnType -> Value -> Bool
validateValue IntegerType (IntegerValue _) = True
validateValue StringType (StringValue _) = True
validateValue BoolType (BoolValue _) = True
validateValue _ NullValue = True
validateValue _ _ = False

--update employees set name = 'ar', id = 100 where surname <= 'Dl';
--UpdateStatement TableName [(ColumnName, Value)] (Maybe Condition)
--executeUpdate :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)

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
      let valuesAreValid = validateValuesForUpdate existingColumns updates


      if valuesAreValid
        then do
          -- Filter rows based on the condition
          let maybeDataFrame = extractDataFrame loadedDF
          let filteredRows = case maybeDataFrame of
                Just df -> Lib2.filterRows existingColumns df condition --returns [Row]
                Nothing -> []  
          -- Update the DataFrame with new values
          let updatedRows = updateRows existingColumns updateColumnNames updates filteredRows
          let newDF = DataFrame existingColumns (replaceRows loadedDF filteredRows updatedRows)

          saveTable tableName newDF
          return $ Right newDF
        else return $ Left $ "Invalid values for columns"
    else return $ Left "Columns to update do not exist in the DataFrame"

validateValuesForUpdate :: [Column] -> [(ColumnName, Value)] -> Bool
validateValuesForUpdate columns updates =
  Data.List.all isValidUpdate updates
  where
    isValidUpdate :: (ColumnName, Value) -> Bool
    isValidUpdate (colName, colValue) =
      case findColumnType colName columns of
        Just colType -> validateValue colType colValue
        Nothing -> False

    findColumnType :: ColumnName -> [Column] -> Maybe ColumnType
    findColumnType name cols = case Data.List.find (\(Column colName _) -> colName == name) cols of
      Just (Column _ colType) -> Just colType
      Nothing -> Nothing


-- Function to replace old rows with updated rows
replaceRows :: Either ErrorMessage DataFrame -> [Row] -> [Row] -> [Row]
replaceRows (Right (DataFrame columns oldRows)) oldFilteredRows newRows =
  let indexedOldRows = Data.List.zip [1..] oldRows
      indexedOldFilteredRows = [(i, row) | (i, row) <- indexedOldRows, row `Data.List.elem` oldFilteredRows]
      indexedNewRows = Data.List.zip (map fst indexedOldFilteredRows) newRows
      combinedRows = map (\(i, oldRow) ->
                            case Data.List.find (\(j, _) -> i == j) indexedNewRows of
                              Just (_, newRow) -> (i, newRow)
                              Nothing -> (i, oldRow)) indexedOldRows
  in map snd combinedRows
  
extractDataFrame :: Either ErrorMessage DataFrame -> Maybe DataFrame
extractDataFrame (Right df) = Just df
extractDataFrame _ = Nothing

-- Function to update a single row based on the provided updates
updateRows :: [Column] -> [ColumnName] -> [(ColumnName, Value)] -> [Row] -> [Row]
updateRows columns colNames updates rows =
  map (updateValues columns colNames updates) rows
  where
    updateValues :: [Column] -> [ColumnName] -> [(ColumnName, Value)] -> Row -> Row
    updateValues columns colNames updates' row =
      map (\col -> case lookup (getColumnName col) updates' of
                     Just updatedValue -> updatedValue
                     Nothing -> getOriginalValue col row updates') columns

    getOriginalValue :: Column -> Row -> [(ColumnName, Value)] -> Value
    getOriginalValue col row' updates'' =
      case lookup (getColumnName col) updates'' of
        Just updatedValue -> updatedValue
        Nothing -> getColumnValue col row'

    getColumnValue :: Column -> Row -> Value
    getColumnValue col row' =
      case Lib2.findColumnIndex columns (getColumnName col) of
        Just colIndex -> row' !! colIndex
        Nothing -> error "Column not found in dataframe"  -- Handle the error case appropriately

    getColumnName :: Column -> ColumnName
    getColumnName (Column name _) = name


--DeleteStatement "employees" (Just (Comparison (Where "surname" LessThanOrEqual "Dl") []))
--delete from employees where surname <= 'Dl';
--DeleteStatement TableName (Maybe Condition) 
executeDelete :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeDelete (Lib2.DeleteStatement tableName condition) = do
  -- Get the existing DataFrame
  loadedDF <- loadFile tableName
  let existingColumns = dataframeColumns loadedDF

  -- Filter rows based on the condition
  let maybeDataFrame = extractDataFrame loadedDF
  let filteredRows = case maybeDataFrame of
        Just df -> Lib2.filterRows existingColumns df condition
        Nothing -> []  -- Handle the error case appropriately
  

  -- Delete the DataFrame with filtered rows
  let newDF = DataFrame existingColumns (replaceRows loadedDF filteredRows [])

  saveTable tableName newDF
  return $ Right newDF


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
    _ | "delete" `isPrefixOf` sql' -> do
        parsedStatement <- parseStatement sql
        executeDelete parsedStatement
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
--our custom df serialization, we call it in the main interpreter
serializeDataFrame :: DataFrame -> String
serializeDataFrame (DataFrame columns rows) =
  "[[ " ++ Data.List.intercalate ", " (Prelude.map serializeColumn columns) ++ " ], " ++
  "[ " ++ Data.List.intercalate ", " (Prelude.map serializeRow rows) ++ " ]]"

-- Deserialization Instances
--instance ToJSON we nee if we want to use aeson `encode` function. so it's kinda our backup plan if something goes wrong
instance FromJSON ColumnType
--instance ToJSON ColumnType

instance FromJSON Column
--instance ToJSON Column

instance FromJSON DataFrame.Value
--instance ToJSON DataFrame.Value

instance FromJSON DataFrame
--instance ToJSON DataFrame
--we can delete this block of code but let's keep it for a while 
-- save :: DataFrame -> TableName -> IO() --we devided this function into 2 functions and actually we are doing this in the main interpeter
-- save df tableName = do
--   let filePath = "db/" ++ tableName ++ ".json"
--   let jsonStr = serializeDataFrame df 
--   Prelude.writeFile filePath jsonStr

-- load :: TableName -> IO DataFrame -- we devided this function into 2 functions, load and deserializeDataFrame
-- load tableName = do
--   let filePath = "db/" ++ tableName ++ ".json"
--   jsonStr <- Prelude.readFile filePath
--   case eitherDecode (BSLC.pack jsonStr) of --decode (eitherDecode in our case) takes a ByteStream as an arguments, that's why we need to convert jsonStr into byteStream
--     Right df -> return df
--     Left err -> error $ "Failed to decode JSON: " ++ err

-- load :: TableName -> IO FileContent --we are doing this in the main interpeter though
-- load tableName = do
--   let filePath = "db/" ++ tableName ++ ".json"
--   Prelude.readFile filePath    

deserializeDataFrame :: FileContent -> Either ErrorMessage DataFrame
deserializeDataFrame jsonStr =
  case eitherDecode (BSLC.pack jsonStr) of
    Right df -> Right df
    Left err -> Left $ "Failed to decode JSON: " ++ err

