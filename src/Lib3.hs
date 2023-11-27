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

data MyColumnType
  = MyIntegerType
  | MyStringType
  | MyBoolType
  deriving (Show, Eq, Generic)

data MyColumn = MyColumn String MyColumnType
  deriving (Show, Eq, Generic)

data MyValue
  = MyIntegerValue Integer
  | MyStringValue String
  | MyBoolValue Bool
  | MyNullValue
  deriving (Show, Eq, Generic)

type MyRow = [MyValue]

data MyDataFrame = MyDataFrame [MyColumn] [MyRow]
  deriving (Show, Eq, Generic)

-- Instances for ToJSON and FromJSON

instance ToJSON MyColumnType
instance FromJSON MyColumnType

instance ToJSON MyColumn
instance FromJSON MyColumn

instance ToJSON MyValue
instance FromJSON MyValue

instance ToJSON MyDataFrame
instance FromJSON MyDataFrame

-- Example usage:

exampleMyDataFrame :: MyDataFrame
exampleMyDataFrame = MyDataFrame
  [MyColumn "Name" MyStringType, MyColumn "Age" MyIntegerType, MyColumn "IsStudent" MyBoolType]
  [ [MyStringValue "Alice", MyIntegerValue 25, MyBoolValue False]
  , [MyStringValue "Bob", MyIntegerValue 30, MyBoolValue True]
  , [MyStringValue "Charlie", MyIntegerValue 22, MyBoolValue True]
  ]

main :: IO ()
main = do
  -- Serialize to JSON
  let jsonStr = encode exampleMyDataFrame
  BS.writeFile "db/my_dataframe.json" jsonStr

  -- Deserialize from JSON
  let decodedMyDataFrame = case decode jsonStr :: Maybe MyDataFrame of
        Just df -> df
        Nothing -> error "Failed to decode JSON"

  print decodedMyDataFrame

