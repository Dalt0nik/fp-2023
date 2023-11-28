{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
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
import Data.Aeson 
import GHC.Generics
import Data.ByteString.Lazy as BS
import GHC.RTS.Flags (ProfFlags(retainerSelector))

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
  = LoadFile TableName (FileContent -> next) -- deserialize table
  | SaveTable (TableName, DataFrame) (()-> next) --serialize table
  | GetCurrentTime (UTCTime -> next)
  | ShowTable TableName (Either ErrorMessage DataFrame -> next)
  | ParseStatement String (Lib2.ParsedStatement -> next)
  | ExecuteStatement Lib2.ParsedStatement (Either ErrorMessage DataFrame -> next)
  deriving Functor


loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id --why id?

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

-- data Player = Player {
--     health :: Int,
--     position :: (Int, Int),
--     name :: String,
--     friends :: [Player]
-- } deriving (Generic, Show) 

-- instance ToJSON Player

-- instance FromJSON Player where
--   parseJSON = withObject "Player" $ \v ->
--     Player
--       <$> v .: "health"  
--       <*> v .: "position"
--       <*> v .: "name"
--       <*> v .: "friends"

-- save :: Player -> IO ()
-- save player = BS.writeFile "player.json" (encode player)


-- load :: IO (Maybe Player)
-- load = do
--   json <- BS.readFile "player.json"
--   return (decode json)


-- Instances for ToJSON and FromJSON
instance ToJSON ColumnType
instance FromJSON ColumnType

instance ToJSON Column
instance FromJSON Column

instance ToJSON DataFrame.Value
instance FromJSON DataFrame.Value

instance ToJSON DataFrame
instance FromJSON DataFrame

-- Example usage:

exampleDataFrame :: DataFrame
exampleDataFrame = DataFrame
  [Column "Name" StringType, Column "Age" IntegerType, Column "IsStudent" BoolType]
  [ [StringValue "Alice", IntegerValue 25, BoolValue False]
  , [StringValue "Bob", IntegerValue 30, BoolValue True]
  , [StringValue "Charlie", IntegerValue 22, BoolValue True]
  ]

-- Save function
save :: DataFrame -> TableName -> IO ()
save df tableName = do
  let filePath = "db/" ++ tableName ++ ".json"
  let jsonStr = encode df
  BS.writeFile filePath jsonStr

load :: TableName -> IO DataFrame
load tableName = do
  let filePath = "db/" ++ tableName ++ ".json"
  jsonStr <- BS.readFile filePath
  case decode jsonStr of
    Just df -> return df
    Nothing -> error "Failed to decode JSON"

main :: IO ()
main = do
  save exampleDataFrame "my_dataframe2"

  loadedDataFrame <- load "my_dataframe2" -- Load from JSON
  print loadedDataFrame

