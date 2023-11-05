{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame (..), Value (..), Column (..), ColumnType (..))
import InMemoryTables (TableName, database)
import Data.Char
import Data.List (isPrefixOf)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data Condition
  = Comparison Column Operator Value
  | Aggregation AggregateFunction Column
  | LogicalOperator Condition LogicalOp Condition

data Operator
  = Equals
  | NotEquals
  | LessThan
  | GreaterThan
  | LessThanOrEqual
  | GreaterThanOrEqual

data AggregateFunction
  = Min
  | Sum

data LogicalOp
  = Or

data ParsedStatement
  = ShowTablesStatement
  | ShowTableStatement TableName
  | SelectStatement [Column] TableName [Condition]

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | map toLower input == "show tables" = Right ShowTablesStatement
  | "show table " `isPrefixOf` map toLower input = do
    let tableName = drop 11 (map toLower input) -- Extract the table name
    Right (ShowTableStatement tableName)
  | "select" `isPrefixOf` map toLower input = do
    let restOfQuery = drop 6 (map toLower input) -- Remove "select"
    parsedQuery <- parseSelectQuery restOfQuery
    return parsedQuery
  | otherwise = Left "Not implemented: parseStatement"

-- Helper function to parse the SELECT statement
parseSelectQuery :: String -> Either ErrorMessage ParsedStatement
parseSelectQuery input = do
  -- Parse the list of columns to select
  let (colString, rest) = break (== ' ') input
  let columns = parseColumns colString

  -- Parse the "from" keyword
  let rest' = dropWhile isSpace rest
  rest'' <- case parseKeyword "from" rest' of
    Right r -> Right r
    Left _ -> Left "Expected 'from' keyword"

  -- Parse the table name
  let (table, rest''') = span (not . isSpace) (dropWhile isSpace rest'')

  -- Parse the conditions (if any)
  conditions <- parseConditions (dropWhile isSpace rest''')

  Right (SelectStatement columns table conditions)

-- Helper function to parse a list of columns
parseColumns :: String -> [Column]
parseColumns input = undefined -- Implement this based on your specific grammar

-- Helper function to parse conditions
parseConditions :: String -> Either ErrorMessage [Condition]
parseConditions input = do
  let (conditionString, rest) = break (== ' ') input
  case conditionString of
    "min" -> do
      let (col, rest') = span (not . isSpace) (dropWhile isSpace rest)
      Right [Aggregation Min (Column col)]
    "sum" -> do
      let (col, rest') = span (not . isSpace) (dropWhile isSpace rest)
      Right [Aggregation Sum (Column col)]
    "where" -> parseWhereConditions rest
    _ -> Left "Invalid condition"

-- Helper function to parse WHERE conditions
parseWhereConditions :: String -> Either ErrorMessage [Condition]
parseWhereConditions input = undefined -- Implement this based on your specific grammar

-- Helper function to parse a keyword
parseKeyword :: String -> String -> Either ErrorMessage String
parseKeyword keyword input
  | map toLower keyword == take (length keyword) (map toLower input) = Right (drop (length keyword) input)
  | otherwise = Left $ keyword ++ " expected"



-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTablesStatement = Right $ DataFrame [Column "Table Name" StringType] (map (\tableName -> [StringValue tableName]) (showTables database))
-- | -- SelectStatement implementation
-- | -- ShowTableStatement implementation
executeStatement _ = Left "Not implemented: executeStatement"

--used in ShowTables execution
showTables :: Database -> [TableName]
showTables db = map fst db