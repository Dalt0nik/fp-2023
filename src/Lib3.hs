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

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetCurrentTime (UTCTime -> next)
  | NowStatement next
  | ShowTable TableName (Either ErrorMessage DataFrame -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getCurrentTime :: Execution UTCTime
getCurrentTime = liftF $ GetCurrentTime id

nowStatement :: Execution ()
nowStatement = liftF $ NowStatement ()

showTable :: TableName -> Execution (Either ErrorMessage DataFrame)
showTable tableName = case Lib2.fetchTableFromDatabase tableName of
  Right (_, table) -> return $ Right table
  Left errMsg -> return $ Left errMsg

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql =
  case words sql of
    ["now()"] -> do
        nowStatement
        currentTime <- getCurrentTime
        let column = Column "time" StringType
            value = StringValue (show currentTime)
        return $ Right $ DataFrame [column] [[value]]
    ["showTable", tableName] -> do
        showTable tableName
    _ -> return $ Left "Unknown command"