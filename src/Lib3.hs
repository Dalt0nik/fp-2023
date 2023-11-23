{-# LANGUAGE DeriveFunctor #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    --showTable,
    findTableByName
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import Data.Time ( UTCTime )
import InMemoryTables (database, TableName)
import Data.Char


type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | GetCurrentTime (UTCTime -> next)
  | NowStatement next
  -- | ShowTable TableName (DataFrame -> next)
  | FindTableByName TableName (Either ErrorMessage DataFrame -> next)
  deriving Functor

type Execution = Free ExecutionAlgebra

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

getCurrentTime :: Execution UTCTime
getCurrentTime = liftF $ GetCurrentTime id

nowStatement :: Execution ()
nowStatement = liftF $ NowStatement ()

-- showTable :: TableName -> (DataFrame -> a) -> Execution a
-- showTable tableName f = liftF $ ShowTable tableName f

findTableByName :: TableName -> Execution (Either ErrorMessage DataFrame)
findTableByName tableName = case fetchTableFromDatabase tableName of
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
    -- ["showTable", tableName] -> do
    --     showTable tableName return
    ["findTable", tableName] -> do
        findTableByName tableName
    _ -> return $ Left "Unknown command"



fetchTableFromDatabase :: TableName -> Either ErrorMessage (TableName, DataFrame)
fetchTableFromDatabase tableName = case lookup (map toLower tableName) database of
    Just table -> Right (map toLower tableName, table)
    Nothing -> Left (tableName ++ " not found")