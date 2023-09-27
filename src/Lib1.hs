{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)
import Data.Char

type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName [] _ = Nothing
findTableByName _ [] = Nothing
findTableByName ((n,f):xs) name = 
  if  map toLower name  ==  map toLower n  then Just f else findTableByName xs name


-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
removeSemicolons :: String -> String
removeSemicolons = filter (/= ';')

parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement sql = case (map toLower sql) of
    ('s':'e':'l':'e':'c':'t':' ':'*':' ':'f':'r':'o':'m':' ':rest) -> 
        case words rest of
            (tableName:_) -> Right (removeSemicolons tableName)
            _ -> Left "Error. Missing table name"
    _ -> Left "Invalid SQL statement: Missing 'SELECT * FROM' statement"

-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame (DataFrame [] _) = Left "DataFrame has no columns"    
validateDataFrame (DataFrame _ []) = Left "DataFrame has no rows"
validateDataFrame dfs =
   case checkLengthOfRows (lengthOfColumn dfs) dfs of
   Right () -> 
    case checkTypesInRows dfs of
      Right () -> Right ()
      Left errorMessage -> Left errorMessage
   Left errorMessage -> Left errorMessage

listLength :: [a] -> Integer
listLength a =
  let
      listLength' :: [a] -> Integer -> Integer
      listLength' [] a' = a'
      listLength' (_:xs) a' = listLength' xs (a' + 1) 
  in listLength' a 0

-- first Integer of the function is the number of columns
checkLengthOfRows :: Integer -> DataFrame -> Either ErrorMessage ()
checkLengthOfRows a (DataFrame _ r) = goThroughRows a r
   where 
       goThroughRows :: Integer -> [Row] -> Either ErrorMessage ()
       goThroughRows _ [] = Right ()
       goThroughRows num (x:xs) =  
        if listLength x == num then goThroughRows a xs else Left (show a ++ " expected but " ++ show (listLength x) ++ " found")

lengthOfColumn :: DataFrame -> Integer
lengthOfColumn (DataFrame c _) = listLength c

extractColumnTypeFromColumn :: Column -> Char
extractColumnTypeFromColumn (Column _ c) = 
  case c of
    IntegerType -> 'I'
    StringType -> 'S'
    BoolType -> 'B'

extractValueTypeFromValue :: Value -> Char
extractValueTypeFromValue v = 
  case v of
    (IntegerValue _) -> 'I'
    (StringValue _) -> 'S'
    (BoolValue _) -> 'B'
    NullValue -> 'N'

checkTypesInRows :: DataFrame -> Either ErrorMessage ()
checkTypesInRows (DataFrame c r) = checkThroughRows c r
  where
    checkThroughRows :: [Column] -> [Row] -> Either ErrorMessage ()
    checkThroughRows _ [] = Right ()
    checkThroughRows column (row:rowLeft) = 
      let
        checkOneRow :: [Column] -> [Value] -> Bool
        checkOneRow [] _ = True
        checkOneRow (_:_) [] = True
        checkOneRow (col:colLeft) (val:valLeft) = 
          if (extractColumnTypeFromColumn col == extractValueTypeFromValue val) || (extractValueTypeFromValue val == 'N') then checkOneRow colLeft valLeft else False
      in if checkOneRow column row then checkThroughRows column rowLeft else Left "Column type mismatches value type"

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"

-- Function to convert a row to a list of strings
rowToList :: Integer -> Row -> [String]
rowToList availableWidth = map (valueToString availableWidth)
  where
    valueToString :: Integer -> Value -> String
    valueToString width NullValue = padString "null" width ++ "|"
    valueToString width (IntegerValue n) = padString (show n) width ++ "|"
    valueToString width (StringValue s) = padString s width ++ "|"
    valueToString width (BoolValue True) = padString "true" width ++ "|"
    valueToString width (BoolValue False) = padString "false" width ++ "|"

    -- Function to pad a string with spaces to a specified width
    padString :: String -> Integer -> String
    padString s width = take (fromIntegral width) (s ++ repeat ' ')

-- Function to concatenate a list of strings without spaces
listToStringWithoutSpaces :: [String] -> String
listToStringWithoutSpaces = concat

-- Function to print the entire table (all rows) of a DataFrame as a single string
printTableAsString :: Integer -> (a, DataFrame) -> IO ()
printTableAsString _ (_, DataFrame _ []) = putStrLn "DataFrame has no rows."
printTableAsString width (_, DataFrame _ rows) = do
    let numberOfColumns = case rows of
            [] -> 0
            (firstRow:_) -> length firstRow
        separatorLine = "+" ++ concat (replicate numberOfColumns (replicate ((fromInteger width - 1) `div` fromIntegral numberOfColumns - 1) '-' ++ "+")) ++ "\n"
        rowStrings = map (\row -> separatorLine ++ "|" ++ listToStringWithoutSpaces (rowToList (((width - 1) `div` fromIntegral numberOfColumns) - 1) row) ++ "\n") rows
        tableAsString = concat rowStrings ++ separatorLine
    putStrLn tableAsString



tableEmployees :: (TableName, DataFrame)
tableEmployees =
  ( "employees",
    DataFrame
      [Column "id" IntegerType, Column "first_name" StringType, Column "last_name" StringType, Column "age" IntegerType]
      [ [IntegerValue 1, NullValue, StringValue "Johnson", IntegerValue 30],
        [IntegerValue 2, StringValue "Bob", StringValue "Smith", IntegerValue 28],
        [IntegerValue 3, StringValue "Charlie", StringValue "Brown", IntegerValue 35],
        [IntegerValue 4, StringValue "David", StringValue "Lee", IntegerValue 40],
        [IntegerValue 5, StringValue "Eve", StringValue "Wilson", IntegerValue 25],
        [IntegerValue 6, StringValue "Frank", StringValue "Davis", IntegerValue 32],
        [IntegerValue 7, StringValue "Grace", StringValue "Miller", IntegerValue 27],
        [IntegerValue 8, StringValue "Hannah", StringValue "Thompson", IntegerValue 42],
        [IntegerValue 9, StringValue "Isaac", StringValue "Brown", IntegerValue 38],
        [IntegerValue 10, StringValue "Jane", StringValue "Adams", IntegerValue 29],
        [IntegerValue 11, StringValue "Kevin", StringValue "Taylor", IntegerValue 33],
        [IntegerValue 12, StringValue "Linda", StringValue "Hall", IntegerValue 45],
        [IntegerValue 13, StringValue "Mike", StringValue "Clark", IntegerValue 31],
        [IntegerValue 14, StringValue "Nancy", StringValue "Lewis", IntegerValue 36],
        [IntegerValue 15, StringValue "Olivia", StringValue "White", IntegerValue 26]
      ]
  )

