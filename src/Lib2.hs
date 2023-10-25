{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame)
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
data StrCoditions = Equal
                  | NotEqual
                  | GreaterOrEqual
                  | LessOrEqual deriving Show

data LogicalOp = Or deriving Show

data WhereStr = WhereStr ColumnName StrCoditions String deriving Show

data WhereStatement = WhereStatement String WhereStr [(LogicalOp, WhereStr)] deriving Show

type ColumnName = String

parseLogicalOp :: Parser LogicalOp
parseLogicalOp = parseKeyword "OR" >> pure Or

parseStrConditions :: Parser StrCoditions
parseStrConditions = parseEqual <|> parseNotEqual <|> parseGreaterOrEqual <|> parseLessOrEqual
  where
    parseEqual :: Parser StrCoditions
    parseEqual = parseKeyword "=" >> pure Equal

    parseNotEqual :: Parser StrCoditions
    parseNotEqual = parseKeyword "<>" >> pure NotEqual

    parseGreaterOrEqual :: Parser StrCoditions
    parseGreaterOrEqual = parseKeyword ">=" >> pure GreaterOrEqual

    parseLessOrEqual :: Parser StrCoditions
    parseLessOrEqual = parseKeyword "<=" >> pure LessOrEqual

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
   whereKeyWord <- parseKeyword "WHERE"
   _ <- parseWhitespace
   columnName <- parseName
   _ <- parseWhitespace
   condition <- parseStrConditions
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
     condition' <- parseStrConditions
     _ <- parseWhitespace
     _ <- parseQuotationMarks
     conditionString' <- parseName
     _ <- parseQuotationMarks
     return (logicalOp, WhereStr columnName' condition' conditionString')
   return $ WhereStatement whereKeyWord (WhereStr columnName condition conditionString) otherConditions

parseQuery :: String -> Either ErrorMessage (String, WhereStatement)
parseQuery = runParser parseWhereStatement

