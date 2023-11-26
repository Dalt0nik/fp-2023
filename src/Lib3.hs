{-# LANGUAGE DeriveFunctor #-}

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
import InMemoryTables (database, TableName)
import Data.Char
import Lib2 qualified


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
  = LoadFile TableName (FileContent -> next)
  | GetCurrentTime (UTCTime -> next)
  | ShowTable TableName (Either ErrorMessage DataFrame -> next)
  | ParseStatement String (Lib2.ParsedStatement -> next)
  | ExecuteStatement Lib2.ParsedStatement (Either ErrorMessage DataFrame -> next)
  deriving Functor


loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

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