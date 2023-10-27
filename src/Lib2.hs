{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Redundant if" #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

--import DataFrame (DataFrame)
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)
import Control.Applicative((<|>), empty, Alternative (some, many))
import Data.Char(isAlphaNum, toLower, isSpace)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

-- Keep the type, modify constructors
data ParsedStatement = ParsedStatement

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
  fmap :: (a -> b) -> Parser a -> Parser b
  fmap f functor = Parser $ \inp ->
    case runParser functor inp of
        Left e -> Left e
        Right (l, a) -> Right (l, f a)

instance Applicative Parser where
  pure :: a -> Parser a
  pure a = Parser $ \inp -> Right (inp, a)
  (<*>) :: Parser (a -> b) -> Parser a -> Parser b
  ff <*> fa = Parser $ \in1 ->
    case runParser ff in1 of
        Left e1 -> Left e1
        Right (in2, f) -> case runParser fa in2 of
            Left e2 -> Left e2
            Right (in3, a) -> Right (in3, f a)

instance Alternative Parser where
  empty :: Parser a
  empty = Parser $ \_ -> Left "Error"
  (<|>) :: Parser a -> Parser a -> Parser a
  p1 <|> p2 = Parser $ \inp ->
    case runParser p1 inp of
        Right r1 -> Right r1
        Left _ -> case runParser p2 inp of
            Right r2 -> Right r2
            Left e -> Left e

instance Monad Parser where
  (>>=) :: Parser a -> (a -> Parser b) -> Parser b
  ma >>= mf = Parser $ \inp1 ->
    case runParser ma inp1 of
        Left e1 -> Left e1
        Right (inp2, a) -> case runParser (mf a ) inp2 of
            Left e2 -> Left e2
            Right (inp3, r) -> Right (inp3, r)

-- Parses user input into an entity representing a parsed
-- statement
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement _ = Left "Not implemented: parseStatement"

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement _ = Left "Not implemented: executeStatement"

------------------------------START OF CHANGES----------------------------
parseName :: Parser String
parseName = Parser $ \inp ->
    case takeWhile isAlphaNum inp of --recognizes only letters and numbers (takeWhile stops when the first char which returns false occurses)
        [] -> Left "Empty input"
        xs -> Right (drop (length xs) inp, xs) --drop (length xs) inp shows what is left in inp after xs

parseChar :: Char -> Parser Char
parseChar a = Parser $ \inp ->
    case inp of
        [] -> Left "Empty input"
        (x:xs) -> if a == x then Right (xs, a) else Left ([a] ++ " expected but " ++ [x] ++ " found")

data Columns = All | ColumntList [String] deriving Show

parseColumns :: Parser Columns
parseColumns = parseAll <|> parseColumnList

parseAll :: Parser Columns
parseAll = fmap (\_ -> All) $ parseChar '*'

parseColumnList :: Parser Columns
parseColumnList = do
    name <- parseName
    other <- many parseCSName
    return $ ColumntList $ name:other

parseCSName :: Parser String
parseCSName = do
    _ <- many $ parseChar ' ' --many means that can be zero or more ' ' chars
    _ <- parseChar ','
    _ <- many $ parseChar ' '
    name <- parseName
    return name

parseColumnListQuery :: String -> Either ErrorMessage (String, Columns)
parseColumnListQuery inp = runParser parseColumns inp

parseKeyword :: String -> Parser String
parseKeyword keyword = Parser $ \inp ->
  if map toLower (take l inp) == map toLower keyword then
    Right (drop l inp, keyword)
  else
    Left $ keyword ++ " expected"
  where
    l = length keyword
----------------MY CODE-----------------------

data Operator = Equals
              | NotEquals
              | LessThanOrEqual
              | GreaterThanOrEqual deriving Show          

data LogicalOp = Or deriving Show

data WhereStr = WhereStr ColumnName Operator String deriving Show

data WhereStatement = WhereStatement WhereStr [(LogicalOp, WhereStr)] deriving Show

type ColumnName = String

parseLogicalOp :: Parser LogicalOp
parseLogicalOp = parseKeyword "OR" >> pure Or

parseOperator :: Parser Operator
parseOperator = parseEquals <|> parseNotEquals <|> parseGreaterThanOrEqual <|> parseLessThanOrEqual
  where
    parseEquals :: Parser Operator
    parseEquals = parseKeyword "=" >> pure Equals

    parseNotEquals :: Parser Operator
    parseNotEquals = parseKeyword "<>" >> pure NotEquals

    parseGreaterThanOrEqual :: Parser Operator
    parseGreaterThanOrEqual = parseKeyword ">=" >> pure GreaterThanOrEqual

    parseLessThanOrEqual :: Parser Operator
    parseLessThanOrEqual = parseKeyword "<=" >> pure LessThanOrEqual

parseWhitespace :: Parser String
parseWhitespace = do
   _ <- some $ parseChar ' ' -- many means that it is 1 or more ' ' chars
   return ""

parseQuotationMarks :: Parser String
parseQuotationMarks = do
  _ <- parseChar '\''
  return ""

parseWhereStatement :: Parser WhereStatement
parseWhereStatement = do
   _ <- parseKeyword "WHERE"
   _ <- parseWhitespace
   columnName <- parseName
   _ <- parseWhitespace
   condition <- parseOperator
   _ <- parseWhitespace
   _ <- parseQuotationMarks
   conditionString <- parseName
   _ <- parseQuotationMarks
   otherConditions <- many $ do
     _ <- parseWhitespace
     logicalOp <- parseLogicalOp
     _ <- parseWhitespace
     columnName' <- parseName
     _ <- parseWhitespace
     condition' <- parseOperator
     _ <- parseWhitespace
     _ <- parseQuotationMarks
     conditionString' <- parseName
     _ <- parseQuotationMarks
     return (logicalOp, WhereStr columnName' condition' conditionString')
   return $ WhereStatement (WhereStr columnName condition conditionString) otherConditions

parseWhere :: String -> Either ErrorMessage (String, WhereStatement)
parseWhere = runParser parseWhereStatement

extractColumnValuesByColumnNames :: DataFrame -> [ColumnName] -> Either ErrorMessage [[Value]]
extractColumnValuesByColumnNames  _ [] = Left "No column name in the output"
extractColumnValuesByColumnNames (DataFrame col rows) colNames = if all (\a -> a /= -1) indexesOfColumns then Right $ map (\i -> extractValues i rows) indexesOfColumns else Left "Column(-s) not found" --all returns true if all elemtnts of a list are true with the condition
  where
    indexesOfColumns = map (extractColumnByColumnName' 0 col) colNames

    extractColumnByColumnName' :: Int -> [Column] -> ColumnName -> Int
    extractColumnByColumnName' _ [] _ = -1
    extractColumnByColumnName' a ((Column colName _):colLeft) cn = if colName == cn then a else extractColumnByColumnName' (a + 1) colLeft cn
    
    extractValues :: Int -> [Row] -> [Value]
    extractValues a r = map (\row -> row !! a) r -- !! extracts needed value from a list by its index

------------------------------VALUE FOR TESTS START------------------------------
tableEmployees :: DataFrame
tableEmployees = DataFrame
    [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
    [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
    [IntegerValue 2, StringValue "Ed", StringValue "Dl"] ]
test :: [Value]
test = [IntegerValue 1, StringValue "Po", IntegerValue 2, StringValue "Ed", StringValue "Dl"]
------------------------------VALUE FOR TESTS END------------------------------
--SELECT name FROM tableEmploees WHERE name = "vi"

compareStrEqual :: String -> String -> Bool
compareStrEqual [] [] = False
compareStrEqual s1 s2 = if length s1 /= length s2 then False else checkByChar s1 s2
  where
    checkByChar :: String -> String -> Bool
    checkByChar [] [] = True
    checkByChar (x1:xs1) (x2:xs2) = if x1 == x2 then checkByChar xs1 xs2 else False

compareStrNotEqual :: String -> String -> Bool
compareStrNotEqual s1 s2 = not (compareStrEqual s1 s2)

isFirstStrGreaterOrEqual :: String -> String -> Bool
isFirstStrGreaterOrEqual [] [] = False
isFirstStrGreaterOrEqual [] _ = False
isFirstStrGreaterOrEqual _ [] = False
isFirstStrGreaterOrEqual s1 s2 = s1 >= s2

isFirstStrLessOrEqual :: String -> String -> Bool
isFirstStrLessOrEqual [] [] = False
isFirstStrLessOrEqual [] _ = False
isFirstStrLessOrEqual _ [] = False
isFirstStrLessOrEqual s1 s2 = s1 <= s2

compareStrEqualWithColumn :: [Value] -> String -> [Value] --DOES NOT CHECK WHETHER [VALUE] IS STRING OR ANY OTHER TYPE, YOU MUST BE SURE THAT [Value] PARAMETER IS STRING!
compareStrEqualWithColumn colVals str = filter (not . isNullValue) (fmap filterValues colVals)
  where
    filterValues :: Value -> Value
    filterValues (StringValue s) = if compareStrEqual s str then StringValue s else NullValue 
    filterValues _ = NullValue

compareStrNotEqualWithColumn :: [Value] -> String -> [Value] --DOES NOT CHECK WHETHER [VALUE] IS STRING OR ANY OTHER TYPE, YOU MUST BE SURE THAT [Value] PARAMETER IS STRING!
compareStrNotEqualWithColumn colVals str = filter (not . isNullValue) (fmap filterValues colVals)
  where
    filterValues :: Value -> Value
    filterValues (StringValue s) = if compareStrNotEqual s str then StringValue s else NullValue 
    filterValues _ = NullValue

compareStrGreaterOrEqualWithColumn :: [Value] -> String -> [Value] --DOES NOT CHECK WHETHER [VALUE] IS STRING OR ANY OTHER TYPE, YOU MUST BE SURE THAT [Value] PARAMETER IS STRING!
compareStrGreaterOrEqualWithColumn colVals str = filter (not . isNullValue) (fmap filterValues colVals)
  where
    filterValues :: Value -> Value
    filterValues (StringValue s) = if isFirstStrGreaterOrEqual s str then StringValue s else NullValue 
    filterValues _ = NullValue

compareStrLessOrEqualWithColumn :: [Value] -> String -> [Value] --DOES NOT CHECK WHETHER [VALUE] IS STRING OR ANY OTHER TYPE, YOU MUST BE SURE THAT [Value] PARAMETER IS STRING!
compareStrLessOrEqualWithColumn colVals str = filter (not.isNullValue) (fmap filterValues colVals)
  where
    filterValues :: Value -> Value
    filterValues (StringValue s) = if isFirstStrLessOrEqual s str then StringValue s else NullValue 
    filterValues _ = NullValue

isNullValue :: Value -> Bool
isNullValue NullValue = True
isNullValue _ = False


  

