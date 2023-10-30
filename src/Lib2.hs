{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Redundant if" #-}
{-# LANGUAGE InstanceSigs #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

--import DataFrame (DataFrame)
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName, database)
import Control.Applicative((<|>), empty, Alternative (some, many))
import Data.Char(isAlphaNum, toLower, isSpace)
import Data.List
import Data.Maybe

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data AggregateFunction
  = Min
  | Sum deriving Show

data Operator = Equals
              | NotEquals
              | LessThanOrEqual
              | GreaterThanOrEqual deriving Show

data LogicalOp = Or deriving Show

data WhereAtomicStatement = Where ColumnName Operator String deriving Show

data Condition
  = Comparison WhereAtomicStatement [(LogicalOp, WhereAtomicStatement)] deriving Show -- string aka column
  

data Columns = All
  | SelectedColumns [String] 
  | Aggregation [(AggregateFunction, String)] deriving Show -- string aka column 

type ColumnName = String
-- type TableName = String
data ParsedStatement
  = ShowTablesStatement
  | ShowTableStatement TableName
  | SelectStatement Columns TableName (Maybe Condition) deriving Show-- Condition

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
parseStatement input
  | map toLower input == "show tables" = Right ShowTablesStatement
  | "show table" `isPrefixOf` map toLower input =
    let tableName = drop 11  (map toLower input)
      in Right  (ShowTableStatement tableName)
  | "select" `isPrefixOf` map toLower input = do
    --let restOfQuery = drop 7 (map toLower input) -- Remove "select " TODO DELETE ALL WHITESPACES
    (restOfQuery,_) <- runParser parseWhitespace $ drop 6 input 
    parsedQuery <- parseSelectQuery restOfQuery
    return parsedQuery
  | otherwise = Left "Not implemented: parseStatement"


parseSelectQuery :: String -> Either ErrorMessage ParsedStatement
parseSelectQuery input = do
    (input', columns) <- parseColumnListQuery input
    (skip,_) <- runParser parseWhitespace input' 
    (input'', tableName) <- parseFromClause skip
    (input''', conditions) <- parseWhere input''
    return (SelectStatement columns tableName conditions)

parseFromClause :: String -> Either ErrorMessage (String, TableName)
parseFromClause input = do
    (rest, _) <- runParser (parseKeyword "FROM") input
    (rest',_) <- runParser parseWhitespace rest 
    (rest'', tableName) <- runParser parseName rest'
    return (rest'', tableName)

-- Parse the list of columns to select until it encounters "FROM" keyword use parseColumnListQuery function 
-- or MIN SUM agregate functions (use parseAggregateFunction function)

-- Parse the "from" keyword

-- Parse the table name

-- Parse the conditions (if any) 
-- use (parseWhere function)

--return   Right (SelectStatement columns table conditions)

-- Executes a parsed statemet. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- Execute a SELECT statement
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame -- SHOW TABLE SHOULD BE ADDED!!!
executeStatement (SelectStatement columns tableName condition) = do
    -- Fetch the specified table
    (tableKey, table) <- fetchTableFromDatabase tableName -- tableKey is tableName in lower case

    -- Filter rows based on the condition
    let filteredRows = filterRows (getColumns table) table condition

    -- Extract the selected column names from the Columns type
    let selectedColumnNames = case columns of
            All -> map extractColumnName (getColumns table)
            SelectedColumns colNames -> colNames

    -- Get the indices of the selected columns
    let selectedColumnIndexes = mapMaybe (\colName -> findColumnIndex (getColumns table) colName) selectedColumnNames

    -- Select columns based on indices
    let selectedColumns = map (\i -> (getColumns table) !! i) selectedColumnIndexes

    -- Extract only the selected values from each row
    let selectedRows = map (\row -> map (\i -> (row !! i)) selectedColumnIndexes) filteredRows

    return $ DataFrame selectedColumns selectedRows
executeStatement ShowTablesStatement = Right $ DataFrame [Column "TABLE NAME" StringType] (map (\tableName -> [StringValue tableName]) (showTables database))
executeStatement (ShowTableStatement tableName) =
  case lookup (map toLower tableName) database of
    Just (DataFrame columns _) -> Right $ DataFrame [Column "COLUMN NAMES" StringType] (map (\col -> [StringValue (extractColumnName col)]) columns)
    Nothing -> Left (tableName ++ " not found")
executeStatement _ = Left "Not implemented: executeStatement"


-- Fetch a table from the database
fetchTableFromDatabase :: TableName -> Either ErrorMessage (TableName, DataFrame)
fetchTableFromDatabase tableName = case lookup (map toLower tableName) database of
    Just table -> Right (map toLower tableName, table)
    Nothing -> Left (tableName ++ " not found")

getColumns :: DataFrame -> [Column]
getColumns (DataFrame columns _) = columns

-- Find the index of a column by its name
findColumnIndex :: [Column] -> ColumnName -> Maybe Int
findColumnIndex columns columnName = elemIndex columnName (map extractColumnName columns)
--   where
--     extractColumnName :: Column -> ColumnName
--     extractColumnName (Column name _) = name

-- Filter rows based on the condition
filterRows :: [Column] -> DataFrame -> Maybe Condition -> [Row]
filterRows columns (DataFrame _ rows) condition = case condition of
    Just (Comparison whereStatement []) -> filter (evaluateWhereStatement whereStatement) rows
    Just (Comparison whereStatement logicalOps) -> filter (evaluateWithLogicalOps whereStatement logicalOps) rows
    --Just (Aggregation _ _) -> rows  -- No filtering needed for aggregation
    Nothing -> rows  -- When condition is Nothing, return all rows
  where
    evaluateWhereStatement :: WhereAtomicStatement -> Row -> Bool
    evaluateWhereStatement (Where columnName op value) row = case op of
        Equals -> extractValue columnName row == value
        NotEquals -> extractValue columnName row /= value
        LessThanOrEqual -> extractValue columnName row <= value
        GreaterThanOrEqual -> extractValue columnName row >= value

    evaluateWithLogicalOps :: WhereAtomicStatement -> [(LogicalOp, WhereAtomicStatement)] -> Row -> Bool
    evaluateWithLogicalOps whereStatement [] row = evaluateWhereStatement whereStatement row
    evaluateWithLogicalOps whereStatement ((Or, nextWhereStatement):rest) row =
        evaluateWhereStatement whereStatement row || evaluateWithLogicalOps nextWhereStatement rest row

    extractValue :: ColumnName -> Row -> String
    extractValue columnName row =
        case findColumnIndex columns columnName of
            Just colIndex -> case row !! colIndex of
                StringValue value -> value
                _ -> ""
            Nothing -> ""

dataFrameColumns :: DataFrame -> [Column]
dataFrameColumns (DataFrame columns _) = columns

-- Select columns
selectColumns :: [Column] -> Columns -> [Column]
selectColumns allColumns (SelectedColumns colNames) =
    filter (\col -> extractColumnName col `elem` colNames) allColumns


--statement3 = SelectStatement (SelectedColumns ["id", "name"]) "employees" (Comparison (Where "surname" Equals "Po") [(Or, Where "name" Equals "Ed")])

------------------------------PARSERS (BEFORE WHERE CLAUSE)----------------------------
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


parseColumns :: Parser Columns
parseColumns = parseAll <|> parseAggregationStatement <|> parseColumnList

parseAll :: Parser Columns
parseAll = fmap (\_ -> All) $ parseChar '*'

parseColumnList :: Parser Columns
parseColumnList = do
    name <- parseName
    other <- many parseCSName
    return $ SelectedColumns $ name : other

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
----------------WHERE PARSER-----------------------
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

parseWhereStatement :: Parser (Maybe Condition)
parseWhereStatement = parseWithWhere <|> parseWithoutWhere
  where
    parseWithWhere = do
       _ <- many parseWhitespace
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
         return (logicalOp, Where columnName' condition' conditionString)
       return $ Just $ Comparison (Where columnName condition conditionString) otherConditions

    parseWithoutWhere = pure Nothing


parseWhere :: String -> Either ErrorMessage (String, Maybe Condition)
parseWhere = runParser parseWhereStatement

-----------MIN SUM----------------------------------------------
parseAggregateFunction :: String -> Either ErrorMessage (String, Columns) -- aka condition aggregation
parseAggregateFunction = runParser parseAggregationStatement

parseAggregationStatement :: Parser Columns
parseAggregationStatement = do
    aggregationFunction <- parseAggregateFunction'
    _ <- parseChar '('
    _ <- many $ parseChar ' '
    columnName <- parseName
    _ <- many $ parseChar ' '
    _ <- parseChar ')'
    other <- many parseCSAggregateFunction
    return $ Aggregation ((aggregationFunction, columnName):other)

parseCSAggregateFunction :: Parser (AggregateFunction, ColumnName)
parseCSAggregateFunction = do
    _ <- many $ parseChar ' ' --many means that can be zero or more ' ' chars
    _ <- parseChar ','
    _ <- many $ parseChar ' '
    aggregationFunction <- parseAggregateFunction'
    _ <- parseChar '('
    columnName <- parseName
    _ <- parseChar ')'
    return (aggregationFunction, columnName)

parseAggregateFunction' :: Parser AggregateFunction
parseAggregateFunction' = parseMin <|> parseSum
  where
    parseMin = parseKeyword "MIN" >> pure Min
    parseSum = parseKeyword "SUM" >> pure Sum



--used in ShowTables execution
showTables :: Database -> [TableName]
showTables db = map fst db

extractColumnName :: Column -> String
extractColumnName (Column name _) = name