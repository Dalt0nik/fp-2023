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
    deserializeDataFrame,
    serializeDataFrame,
    executeStatement
  )
where

import Data.Either (rights)
import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import Data.Time ( UTCTime )
import InMemoryTables (TableName, tableEmployees, database)
import Lib2 qualified
import Data.Aeson hiding (Value) 
import qualified Data.ByteString.Lazy.Char8 as BSLC
import Data.List
import Data.Char
import Lib2 (ParsedStatement(InsertStatement))
import Data.Maybe
import Debug.Trace
import System.Directory



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
  | GetAllTables () (Either ErrorMessage [FilePath] -> next)
  deriving Functor

getAllTables :: Execution (Either ErrorMessage [FilePath])
getAllTables = liftF $ GetAllTables () id


executeShowTables :: Execution (Either ErrorMessage DataFrame)
executeShowTables = do
  tablePaths <- getAllTables
  case tablePaths of
    Left errorMessage -> return $ Left errorMessage
    Right paths -> do

      let tableNames = map (\file -> takeWhile (/= '.') file) (filter (\file -> ".json" `isSuffixOf` file) paths)

      let columnName = Column "Table Names" StringType
      let rows = map (\name -> [StringValue name]) tableNames
      let resultDataFrame = DataFrame [columnName] rows

      return $ Right resultDataFrame


loadFile :: TableName -> Execution (Either ErrorMessage DataFrame)
loadFile tableName = liftF $ LoadFile tableName id

saveTable :: TableName -> DataFrame -> Execution ()
saveTable tableName dataFrame = liftF $ SaveTable (tableName, dataFrame) id


getCurrentTime :: Execution UTCTime
getCurrentTime = liftF $ GetCurrentTime id

parseStatement :: String -> Execution Lib2.ParsedStatement
parseStatement input = liftF $ ParseStatement input id

showTable :: TableName -> Execution (Either ErrorMessage DataFrame)
showTable tableName = case fetchTableFromDatabase tableName of
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
                Just df -> filterRows existingColumns df condition --returns [Row]
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
      case findColumnIndex columns (getColumnName col) of
        Just colIndex -> row' !! colIndex
        Nothing -> error "Column not found in dataframe"  -- Handle the error case appropriately

    getColumnName :: Column -> ColumnName
    getColumnName (Column name _) = name


--DeleteStatement "employees" (Just (Comparison (Where "surname" LessThanOrEqual "Dl") []))
--delete from employees where surname <= 'Dl';
--DeleteStatement TableName (Maybe Condition) 
-- Function to execute a DELETE statement
executeDelete :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeDelete (Lib2.DeleteStatement tableName condition) = do
  -- Get the existing DataFrame
  loadedDF <- loadFile tableName
  let existingColumns = dataframeColumns loadedDF

  -- Filter rows based on the condition
  let maybeDataFrame = extractDataFrame loadedDF
  let filteredRows = case maybeDataFrame of
        Just df -> filterRows existingColumns df condition
        Nothing -> []  -- Handle the error case appropriately

  -- Delete the DataFrame with filtered rows
  let newRows = removeFilteredRows (dataframeRows loadedDF) filteredRows
  let newDF = DataFrame existingColumns newRows

  saveTable tableName newDF -- this line causes fp2023-manipulate: db/employees.json: withFile: resource busy (file is locked) error
  return $ Right newDF
  where
    -- Function to remove rows based on filtering
    removeFilteredRows :: [Row] -> [Row] -> [Row]
    removeFilteredRows rows filtered = filter (not . (`Data.List.elem` filtered)) rows



executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
  let sql' = map toLower (filter (not . isSpace) sql)
  case sql' of
    _ | "showtables" `isPrefixOf` sql' -> executeShowTables
    _ | "select" `isPrefixOf` sql' -> do
      let restOfSql = drop 6 sql'
      case restOfSql of
        _ | "now()" `isPrefixOf` restOfSql -> do
          currentTime <- getCurrentTime
          let column = Column "time" StringType
              value = StringValue (show currentTime)
          return $ Right $ DataFrame [column] [[value]]
        _ -> do
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
    -- _ | "createtable" `isPrefixOf` sql' -> do
    --     parsedStatement <- parseStatement sql
    --     executeCreateTable parsedStatement
    -- _ | "droptable" `isPrefixOf` sql' -> do
    --     let restOfSql = drop 9 sql'
    --     let tableName = takeWhile (/= ';') restOfSql
    --     executeDropTable tableName        
    _ | "showtable" `isPrefixOf` sql' -> do
        let restOfSql = drop 9 sql'
        let tableName = takeWhile (/= ';') restOfSql
        executeShowTable tableName        
    _ -> do
      return $ Left "command not found"

executeShowTable :: TableName -> Execution (Either ErrorMessage DataFrame)
executeShowTable tableName = do
  -- Load the DataFrame with the given table name
  loadedDF <- loadFile tableName

  -- Extract column names and types from the loaded DataFrame
  let columns = dataframeColumns loadedDF
  let rows = map (\(Column name colType) -> [StringValue name, StringValue (show colType)]) columns

  -- Create a new DataFrame with two columns: "Column Name" and "Column Type"
  let columnNameColumn = Column "Column Name" StringType
  let columnTypeColumn = Column "Column Type" StringType
  let resultDataFrame = DataFrame [columnNameColumn, columnTypeColumn] rows

  return $ Right resultDataFrame




  


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
  

deserializeDataFrame :: FileContent -> Either ErrorMessage DataFrame
deserializeDataFrame jsonStr =
  case eitherDecode (BSLC.pack jsonStr) of
    Right df -> Right df
    Left err -> Left $ "Failed to decode JSON: " ++ err


--EXECUTE SELECT FROM LIB2

 -- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- Execute a SELECT statement
executeStatement :: Lib2.ParsedStatement -> Execution (Either ErrorMessage DataFrame)
executeStatement (Lib2.SelectStatement columns tableNames maybeCondition maybeOrder) = do

  --Check whether column tables match fetched tables

  let columnSourceTables = map fst $ getSourcesFromColumns columns
  if not (all (\sourceTable -> sourceTable `elem` tableNames) columnSourceTables)
    then return $ Left "Invalid source table in columns"
  else do
    tableDataList <- mapM loadFile tableNames
    let tablesDF = rights tableDataList
    let combinedList = zip tableNames tablesDF
    
    let isAggregationRequested = case columns of
          Lib2.Aggregation _ -> True
          _ -> False

    -- Check if joining tables is requested
    let numberOfTables = length tableNames
    --if numberOfTables 
    let isJoinRequested = case maybeCondition of
          Just condition -> involvesMultipleTables condition
          _ -> False

    -- Perform inner join if requested
    if isJoinRequested then
      if numberOfTables == 2 then executeJoin combinedList maybeCondition columns isAggregationRequested else return $ Left "only two tables can be joined"
    else
      if numberOfTables /= 1 then return (Left "only one table should be provided") else executeNoJoin combinedList maybeCondition columns isAggregationRequested

executeStatement Lib2.ShowTablesStatement = return $ Right $ DataFrame [Column "TABLE NAME" StringType] (map (\tableName -> [StringValue tableName]) (Lib2.showTables database))
executeStatement (Lib2.ShowTableStatement tableName) =
  case lookup (map toLower tableName) database of
    Just (DataFrame columns _) -> return $ Right $ DataFrame [Column "COLUMN NAMES" StringType] (map (\col -> [StringValue (Lib2.extractColumnName col)]) columns)
    Nothing -> return $ Left (tableName ++ " not found")
executeStatement _ = return $ Left "Not implemented: executeStatement"

getSourcesFromColumns :: Lib2.Columns -> [(TableName, ColumnName)]
getSourcesFromColumns columns =
    case columns of
        Lib2.All -> []
        Lib2.SelectedColumns colNames -> map (\(tableName, columnName) -> (tableName, columnName)) colNames

-- -- Function to execute aggregation functions
executeAggregationFunction :: Lib2.AggregateFunction -> Maybe Int -> [Row] -> Value
executeAggregationFunction Lib2.Min (Just colIndex) rows =
    let values = map (extractValueAtIndex colIndex) rows
    in case minimumValue values of
        Just result -> IntegerValue result
        Nothing -> NullValue
executeAggregationFunction Lib2.Sum (Just colIndex) rows =
    let values = map (extractValueAtIndex colIndex) rows
    in case sumIntValues values of
        Just result -> IntegerValue result
        Nothing -> NullValue
executeAggregationFunction _ _ _ = NullValue

-- Helper function to find the minimum value from a list of Values
minimumValue :: [Value] -> Maybe Integer
minimumValue values =
    case catMaybes [getIntValue value | value <- values] of
        [] -> Nothing
        ints -> Just (minimum ints)

getIntValue :: Value -> Maybe Integer
getIntValue (IntegerValue i) = Just i
getIntValue _ = Nothing

extractValueAtIndex :: Int -> Row -> Value
extractValueAtIndex index row
    | index >= 0 && index < length row = row !! index
    | otherwise = NullValue

sumIntValues :: [Value] -> Maybe Integer
sumIntValues values =
    let intValues = [i | IntegerValue i <- values]
    in if null intValues then Nothing else Just (sum intValues)

createAggregationColumns :: [(Lib2.AggregateFunction, String)] -> [Column]
createAggregationColumns funcs = map (\(aggFunc, colName) -> Column (show aggFunc ++ "(" ++ colName ++ ")") IntegerType) funcs


fetchTableFromDatabase :: TableName -> Either ErrorMessage (TableName, DataFrame)
fetchTableFromDatabase tableName = case lookup (map toLower tableName) database of
    Just table -> Right (map toLower tableName, table)
    Nothing -> Left (tableName ++ " not found")

getColumns :: DataFrame -> [Column]
getColumns (DataFrame columns _) = columns

getColumnsWithTableName :: TableName -> DataFrame -> [(TableName, Column)]
getColumnsWithTableName tableName (DataFrame columns _) = map (\col -> (tableName, col)) columns

findColumnIndex :: [Column] -> ColumnName -> Maybe Int
findColumnIndex columns columnName = elemIndex columnName (map Lib2.extractColumnName columns)


filterRows :: [Column] -> DataFrame -> Maybe Lib2.Condition -> [Row]
filterRows columns (DataFrame _ rows) condition = case condition of
    Just (Lib2.Comparison whereStatement []) -> filter (evaluateWhereStatement whereStatement) rows
    Just (Lib2.Comparison whereStatement logicalOps) -> filter (evaluateWithLogicalOps whereStatement logicalOps) rows
    Nothing -> rows  -- When condition is Nothing, return all rows
  where
    evaluateWhereStatement :: Lib2.WhereAtomicStatement -> Row -> Bool
    evaluateWhereStatement (Lib2.Where (tableName, columnName) op valueEither) row = case op of
        Lib2.Equals -> case valueEither of
            Right (StringValue stringValue) -> extractValue columnName row == stringValue
        Lib2.NotEquals -> case valueEither of
            Right (StringValue stringValue) -> extractValue columnName row /= stringValue
        Lib2.LessThanOrEqual -> case valueEither of
            Right (StringValue stringValue) -> extractValue columnName row <= stringValue
        Lib2.GreaterThanOrEqual -> case valueEither of
            Right (StringValue stringValue) -> extractValue columnName row >= stringValue

    evaluateWithLogicalOps :: Lib2.WhereAtomicStatement -> [(Lib2.LogicalOp, Lib2.WhereAtomicStatement)] -> Row -> Bool
    evaluateWithLogicalOps whereStatement [] row = evaluateWhereStatement whereStatement row
    evaluateWithLogicalOps whereStatement ((Lib2.Or, nextWhereStatement):rest) row =
        evaluateWhereStatement whereStatement row || evaluateWithLogicalOps nextWhereStatement rest row

    extractValue :: ColumnName -> Row -> String
    extractValue columnName row =
        case findColumnIndex columns columnName of
            Just colIndex -> case row !! colIndex of
                StringValue value -> value
                _ -> ""
            Nothing -> ""

dataFrameColumns :: DataFrame -> [Column]
dataFrameColumns (DataFrame columns _) = columns

-- Select columns
-- selectColumns :: [Column] -> Columns -> [Column]
-- selectColumns allColumns (SelectedColumns colNames) =

-- Function to check if a condition involves multiple tables (MUST BE FIRST)
involvesMultipleTables :: Lib2.Condition -> Bool
involvesMultipleTables (Lib2.Comparison whereStatement _) = involvesMultipleTablesInWhere whereStatement -- || any (\(_, cond) -> involvesMultipleTablesInWhere cond) rest --join part must be always the first in where clause
involvesMultipleTables _ = False

-- Function to check if a WHERE clause involves multiple tables
involvesMultipleTablesInWhere :: Lib2.WhereAtomicStatement -> Bool
involvesMultipleTablesInWhere (Lib2.Where (tableName1, _) _ (Left (tableName2, _))) = tableName1 /= tableName2
involvesMultipleTablesInWhere _ = False

-- Function to execute an inner join
executeJoin :: [(TableName, DataFrame)] -> Maybe Lib2.Condition -> Lib2.Columns -> Bool -> Execution (Either ErrorMessage DataFrame)
executeJoin tableDataList maybeCondition columns isAggregationRequested = do
    -- Extract tables and condition
    let (table1Name, table1) = head tableDataList
    let (table2Name, table2) = head (tail tableDataList)
    --let joinCondition = fromMaybe (error "Invalid join condition") maybeCondition -- ???

    -- Ensure join tables have the same column
    let (joinColumnNameTable1, joinColumnNameTable2) = case maybeCondition of
            Just (Lib2.Comparison (Lib2.Where (tableName1, columnName1) _ (Left (tableName2, columnName2))) _) ->
              if tableName1 == table1Name then (columnName1, columnName2)
              else if tableName1 == table2Name then (columnName2, columnName1)
              else error "Invalid join condition"
            Just _ -> error "Invalid join condition"
            Nothing -> error "Invalid join condition"

    -- Fetch the columns to be selected from the join
    let selectedColumnsTable1 = getColumns table1
    let selectedColumnsTable2 = getColumns table2

    let selectedColumnsTable1WithTableName = getColumnsWithTableName table1Name table1
    let selectedColumnsTable2WithTableName = getColumnsWithTableName table2Name table2

    joinColumnIndexTable1 <- either (const (return 0)) return $ case findColumnIndex selectedColumnsTable1 joinColumnNameTable1 of
      Just index -> Right index
      Nothing -> Left "Column not found in table1"

    joinColumnIndexTable2 <- either (const (return 0)) return $ case findColumnIndex selectedColumnsTable2 joinColumnNameTable2 of
      Just index -> Right index
      Nothing -> Left "Column not found in table2"


    -- let resultColumns = case columns of
    --       Lib2.All -> selectedColumnsTable1 ++ selectedColumnsTable2
    --       Lib2.SelectedColumns colNames ->
    --         selectedColumnsTable1 ++ filter (\col -> Lib2.extractColumnName col `elem` map snd colNames) selectedColumnsTable2
    let (filteredColumnsTable1, filteredColumnsTable2) = case columns of
          Lib2.All -> (selectedColumnsTable1, selectedColumnsTable2)
          Lib2.SelectedColumns colNames ->
            let 
              filterCondition (tableName, col) = 
                any (\(selectedTableName, selectedColName) -> selectedTableName == tableName && selectedColName == col) colNames
              filteredColumnsTable1WithTableName = filter (filterCondition . (\(table, col) -> (table, Lib2.extractColumnName col))) selectedColumnsTable1WithTableName
              filteredColumnsTable2WithTableName = filter (filterCondition . (\(table, col) -> (table, Lib2.extractColumnName col))) selectedColumnsTable2WithTableName
              filteredColumnsTable1 = map snd filteredColumnsTable1WithTableName  
              filteredColumnsTable2 = map snd filteredColumnsTable2WithTableName  
            in (filteredColumnsTable1, filteredColumnsTable2)
    
    let indicesTable1 = map (\col -> findColumnIndex selectedColumnsTable1 (Lib2.extractColumnName col)) filteredColumnsTable1
    let indicesTable2 = map (\col -> findColumnIndex selectedColumnsTable2 (Lib2.extractColumnName col)) filteredColumnsTable2

    let indecesOfColumns = case columns of
          Lib2.All -> indicesTable1 ++ map (fmap (+ length indicesTable1)) indicesTable2
          Lib2.SelectedColumns _ -> map (\(tableName, col) -> case tableName of
                              t | t == table1Name -> findColumnIndex selectedColumnsTable1 col
                                | t == table2Name -> Just $ fromMaybe (error "Column not found") (fmap (+ length selectedColumnsTable1) (findColumnIndex selectedColumnsTable2 col))
                                | otherwise -> error ("Unknown table name: " ++ t)
                            ) $ case columns of 
                                  Lib2.SelectedColumns col -> col
--traceShowM ("Indices of columns: " ++ show indecesOfColumns)

    let joinedRows = innerJoin indecesOfColumns joinColumnIndexTable1 joinColumnIndexTable2 table1 table2 maybeCondition

    --let joinedRows = innerJoin indicesTable1 indicesTable2 joinColumnIndexTable1 joinColumnIndexTable2 table1 table2 maybeCondition

    -- Create a new DataFrame with selected columns and joined rows
    --let resultDataFrame = DataFrame resultColumns joinedRows
    let resultDataFrame = DataFrame (filteredColumnsTable1++filteredColumnsTable2) joinedRows

    -- Perform aggregation if requested
    if isAggregationRequested
        then executeAggregation resultDataFrame joinedRows columns
        else return $ Right resultDataFrame

-- Function to perform inner join
innerJoin :: [Maybe Int] -> Int -> Int -> DataFrame -> DataFrame -> Maybe Lib2.Condition -> [Row]
innerJoin indecesOfColumns joinColumnIndexTable1 joinColumnIndexTable2 table1 table2 maybeCondition =
  let rowsTable1 = rowsWithIndixes table1
      rowsTable2 = rowsWithIndixes table2
      matchingRows = filter (\(index1, row1, index2, row2) -> evaluateJoinCondition (index1, row1) (index2, row2)) (joinedRows rowsTable1 rowsTable2) -- leaves only needed columns in a row
  in map (\(_, row1, _, row2) -> createJoinedRow indecesOfColumns (row1++row2)) matchingRows
  where
    rowsWithIndixes :: DataFrame -> [(Int, Row)]
    rowsWithIndixes (DataFrame _ rows) = zip [0..] rows

    joinedRows :: [(Int, Row)] -> [(Int, Row)] -> [(Int, Row, Int, Row)]
    joinedRows list1 list2 = [(index1, row1, index2, row2) | (index1, row1) <- list1, (index2, row2) <- list2]

    evaluateJoinCondition :: (Int, Row) -> (Int, Row) -> Bool
    evaluateJoinCondition (index1, row1) (index2, row2) =
      case maybeCondition of
        Just (Lib2.Comparison whereStatement rest) ->
          let result = evaluateWhereStatement whereStatement (row1 ++ row2)
          in result--trace ("Join condition result: " ++ show result ++ ", row1: " ++ show row1 ++ ", row2: " ++ show row2) result
        Nothing -> trace ("Join condition result (no condition): " ++ show (index1 == index2)) (index1 == index2)

    evaluateWhereStatement :: Lib2.WhereAtomicStatement -> Row -> Bool
    evaluateWhereStatement (Lib2.Where (tableName, columnName) op valueEither) row =
      let
        extractValue' colIndex =
          if colIndex < length row
            then row !! colIndex
            else NullValue
        value1 = extractValue' joinColumnIndexTable1
        value2 = extractValue' (length (getColumns table1) + joinColumnIndexTable2) -- extracting value from the second table (num.of col. of 1st table + index in 2nd table)
        
      in --trace ("Value1: " ++ show value1 ++ ", Value2: " ++ show value2) $
      case op of
        Lib2.Equals ->
            case valueEither of
              Left (tableName2, columnName2) -> value1 == value2
        _ -> error "Unsupported operator for join condition"

    createJoinedRow :: [Maybe Int] -> Row -> Row
    createJoinedRow indecesOfColumns rows =
      let
        selectedValues = map (\index ->
          case index of
            Just i -> rows !! i
            Nothing -> NullValue
            ) indecesOfColumns
      in selectedValues

-- Function to execute selection without join
executeNoJoin :: [(TableName, DataFrame)] -> Maybe Lib2.Condition -> Lib2.Columns -> Bool -> Execution (Either ErrorMessage DataFrame)
executeNoJoin tableDataList maybeCondition columns isAggregationRequested = do
  -- Fetch the specified table
  let (tableName, table) = head tableDataList

  -- Filter rows based on the condition
  let filteredRows = filterRows (getColumns table) table maybeCondition

  -- Perform aggregation if requested
  if isAggregationRequested
      then executeAggregation table filteredRows columns
      else executeSelection table filteredRows columns


executeAggregation :: DataFrame -> [Row] -> Lib2.Columns -> Execution (Either ErrorMessage DataFrame)
executeAggregation table filteredRows columns = do
  let aggregationFunctions = case columns of
          Lib2.Aggregation funcs -> funcs
          _ -> []

  let resultRow = map (\(aggFunc, colName) -> executeAggregationFunction aggFunc (findColumnIndex (getColumns table) colName) filteredRows) aggregationFunctions
  return $ Right $ DataFrame (createAggregationColumns aggregationFunctions) [resultRow]

executeSelection :: DataFrame -> [Row] -> Lib2.Columns -> Execution (Either ErrorMessage DataFrame)
executeSelection table filteredRows columns = do
    let selectedColumnNames = case columns of
            Lib2.All -> map Lib2.extractColumnName (getColumns table)
            Lib2.SelectedColumns colNames -> map snd colNames
    let selectedColumnIndexes = mapMaybe (\colName -> findColumnIndex (getColumns table) colName) selectedColumnNames

    let selectedColumns = map (\i -> (getColumns table) !! i) selectedColumnIndexes

    let selectedRows = map (\row -> map (\i -> (row !! i)) selectedColumnIndexes) filteredRows

    return $ Right $ DataFrame selectedColumns selectedRows



