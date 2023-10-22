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
  -- | -- SHOW TABLE name parsing
  -- | -- SELECT parsing
  | otherwise = Left "Not implemented: parseStatement"


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