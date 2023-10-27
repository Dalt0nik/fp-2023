{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame (..), Value (..), Column (..), ColumnType (..))
import InMemoryTables (TableName, database)
import Data.Char (toLower)
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
  | SelectStatement [Column] TableName Condition

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input
  | map toLower input == "show tables" = Right ShowTablesStatement
  | "show table" `isPrefixOf` map toLower input =
    let tableName = drop 11  (map toLower input)
      in Right  (ShowTableStatement tableName)
  -- | -- SELECT parsing
  | otherwise = Left "Not implemented: parseStatement"


-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTablesStatement = Right $ DataFrame [Column "TABLE NAME" StringType] (map (\tableName -> [StringValue tableName]) (showTables database))
executeStatement (ShowTableStatement tableName) =
  case lookup (map toLower tableName) database of
    Just (DataFrame columns _) -> Right $ DataFrame [Column "COLUMN NAMES" StringType] (map (\col -> [StringValue (extractColumnName col)]) columns)
    Nothing -> Left (tableName ++ " not found")
-- | -- SelectStatement implementation
executeStatement _ = Left "Not implemented: executeStatement"

--used in ShowTables execution
showTables :: Database -> [TableName]
showTables db = map fst db

extractColumnName :: Column -> String
extractColumnName (Column name _) = name