{-# LANGUAGE DeriveGeneric #-}
module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where
import GHC.Generics
data ColumnType
  = IntegerType
  | StringType
  | BoolType
  deriving (Show, Eq, Generic)

data Column = Column String ColumnType
  deriving (Show, Eq, Generic)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  deriving (Show, Eq, Generic)

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq, Generic)
