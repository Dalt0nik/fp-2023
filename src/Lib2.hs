{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Redundant if" #-}
{-# LANGUAGE InstanceSigs #-}

module Lib2 where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName, database)
import Control.Applicative((<|>), empty, Alternative (some, many))
import Data.Char(isAlphaNum, toLower, isSpace, isDigit, isAlpha)
import Data.List
import Data.Maybe
import Text.Read (Lexeme(String))

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

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

type ColumnName = String

data ParsedStatement
  = ShowTablesStatement
  | ShowTableStatement TableName
  | SelectStatement Columns TableName (Maybe Condition)
  | InsertStatement TableName [ColumnName] [[Value]]
  | UpdateStatement TableName [(ColumnName, Value)] (Maybe Condition)
  | DeleteStatement TableName (Maybe Condition) deriving (Show, Eq)-- Condition



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
parseStatement input = do
  let input' = map toLower (filter (not . isSpace) input)
  case input' of
    "showtables" -> Right ShowTablesStatement
    _ | "showtable" `isPrefixOf` input' -> do
        let tableName = drop 9 input'
        Right (ShowTableStatement tableName)
    _ | "select" `isPrefixOf` input' -> do
        (restOfQuery,_) <- runParser parseWhitespace $ drop 6 input
        parsedQuery <- parseSelectQuery restOfQuery
        Right parsedQuery
    _ | "insert" `isPrefixOf` input' -> do
        parsedInsert <- parseInsert input
        Right parsedInsert
    _ | "update" `isPrefixOf` input' -> do
        parsedUpdate <- parseUpdate input
        Right parsedUpdate
    _ | "delete" `isPrefixOf` input' -> do
        parsedDelete <- parseDelete input
        Right parsedDelete
    _ -> Left "Not implemented: parseStatement"


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


-- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- Execute a SELECT statement
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (SelectStatement columns tableName condition) = do
    -- Fetch the specified table
    (tableKey, table) <- fetchTableFromDatabase tableName -- tableKey is tableName in lower case

    -- Filter rows based on the condition
    let filteredRows = filterRows (getColumns table) table condition

    -- Check if aggregation is requested
    let isAggregationRequested = case columns of
            Aggregation _ -> True
            _ -> False

    if isAggregationRequested
        then do
            let aggregationFunctions = case columns of
                    Aggregation funcs -> funcs
                    _ -> []

            let resultRow = map (\(aggFunc, colName) -> executeAggregationFunction aggFunc (findColumnIndex (getColumns table) colName) filteredRows) aggregationFunctions

            return $ DataFrame (createAggregationColumns aggregationFunctions) [resultRow]
        else do

            let selectedColumnNames = case columns of
                    All -> map extractColumnName (getColumns table)
                    SelectedColumns colNames -> colNames

            let selectedColumnIndexes = mapMaybe (\colName -> findColumnIndex (getColumns table) colName) selectedColumnNames

            let selectedColumns = map (\i -> (getColumns table) !! i) selectedColumnIndexes

            let selectedRows = map (\row -> map (\i -> (row !! i)) selectedColumnIndexes) filteredRows

            return $ DataFrame selectedColumns selectedRows
executeStatement ShowTablesStatement = Right $ DataFrame [Column "TABLE NAME" StringType] (map (\tableName -> [StringValue tableName]) (showTables database))
executeStatement (ShowTableStatement tableName) =
  case lookup (map toLower tableName) database of
    Just (DataFrame columns _) -> Right $ DataFrame [Column "COLUMN NAMES" StringType] (map (\col -> [StringValue (extractColumnName col)]) columns)
    Nothing -> Left (tableName ++ " not found")
executeStatement _ = Left "Not implemented: executeStatement"

-- Function to execute aggregation functions
executeAggregationFunction :: AggregateFunction -> Maybe Int -> [Row] -> Value
executeAggregationFunction Min (Just colIndex) rows =
    let values = map (extractValueAtIndex colIndex) rows
    in case minimumValue values of
        Just result -> IntegerValue result
        Nothing -> NullValue
executeAggregationFunction Sum (Just colIndex) rows =
    let values = map (extractValueAtIndex colIndex) rows
    in case sumIntValues values of
        Just result -> IntegerValue result
        Nothing -> NullValue
executeAggregationFunction _ _ _ = NullValue

-- Helper function to find the minimum value from a list of Values
minimumValue :: [Value] -> Maybe Integer
minimumValue values =
    case catMaybes [getIntValue value | value <- values] of
        [] -> Nothing
        ints -> Just (minimum ints)

getIntValue :: Value -> Maybe Integer
getIntValue (IntegerValue i) = Just i
getIntValue _ = Nothing

extractValueAtIndex :: Int -> Row -> Value
extractValueAtIndex index row
    | index >= 0 && index < length row = row !! index
    | otherwise = NullValue

sumIntValues :: [Value] -> Maybe Integer
sumIntValues values =
    let intValues = [i | IntegerValue i <- values]
    in if null intValues then Nothing else Just (sum intValues)

createAggregationColumns :: [(AggregateFunction, String)] -> [Column]
createAggregationColumns funcs = map (\(aggFunc, colName) -> Column (show aggFunc ++ "(" ++ colName ++ ")") IntegerType) funcs


fetchTableFromDatabase :: TableName -> Either ErrorMessage (TableName, DataFrame)
fetchTableFromDatabase tableName = case lookup (map toLower tableName) database of
    Just table -> Right (map toLower tableName, table)
    Nothing -> Left (tableName ++ " not found")

getColumns :: DataFrame -> [Column]
getColumns (DataFrame columns _) = columns


findColumnIndex :: [Column] -> ColumnName -> Maybe Int
findColumnIndex columns columnName = elemIndex columnName (map extractColumnName columns)


filterRows :: [Column] -> DataFrame -> Maybe Condition -> [Row]
filterRows columns (DataFrame _ rows) condition = case condition of
    Just (Comparison whereStatement []) -> filter (evaluateWhereStatement whereStatement) rows
    Just (Comparison whereStatement logicalOps) -> filter (evaluateWithLogicalOps whereStatement logicalOps) rows
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

-- parseKeyword :: String -> Parser String -- OLD VERSION
-- parseKeyword keyword = Parser $ \inp ->
--   if map toLower (take l inp) == map toLower keyword then
--     Right (drop l inp, keyword)
--   else
--     Left $ keyword ++ " expected"
--   where
--     l = length keyword

parseKeyword :: String -> Parser String -- NEW VERSION
parseKeyword keyword = Parser $ \inp ->
  let
    l = length keyword
  in if map toLower (take l inp) == map toLower keyword &&
        (null (drop l inp) || not (isAlpha (head (drop l inp)))) then
        Right (drop l inp, keyword)
      else
        Left $ keyword ++ " expected"

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
   _ <- some $ parseChar ' ' -- some means that it is 1 or more ' ' chars
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
       _ <- many parseWhitespace
       condition <- parseOperator
       _ <- many parseWhitespace
       _ <- parseQuotationMarks
       conditionString <- parseName
       _ <- parseQuotationMarks
       otherConditions <- many $ do
         _ <- parseWhitespace
         logicalOp <- parseLogicalOp
         _ <- many parseWhitespace
         columnName' <- parseName
         _ <- many parseWhitespace
         condition' <- parseOperator
         _ <- many parseWhitespace
         _ <- parseQuotationMarks
         conditionString' <- parseName
         _ <- parseQuotationMarks
         return (logicalOp, Where columnName' condition' conditionString')
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


-- LAB3
parseValue :: Parser Value
parseValue =
  IntegerValue <$> parseInteger <|>
  BoolValue <$> parseBool <|>
  parseNull <|>
  StringValue <$> parseString
    where
      parseInteger :: Parser Integer
      parseInteger = Parser $ \inp ->
        let
          (digits, rest) = span isDigit inp
        in case reads digits of
              [(n, "")] -> Right (rest, n)
              _ -> Left "Error: Not a valid integer"

      parseBool :: Parser Bool
      parseBool = do
        keyword <- parseKeyword "True" <|> parseKeyword "False"
        case map toLower keyword of
          "true"  -> pure True
          "false" -> pure False
          _ -> empty

      parseString :: Parser String
      parseString = do
        _ <- parseQuotationMarks
        string <- parseName
        _ <- parseQuotationMarks
        return string

      parseNull :: Parser Value
      parseNull = parseKeyword "NULL" >> pure NullValue

-- PARSING INSERT -- ALL VALUES MUST CORRESPOND THEIR COLUMNS
--data InsertStatement = Insert TableName [ColumnName] [[Value]] deriving (Show, Eq)

parseInsertStatement :: Parser ParsedStatement
parseInsertStatement = do
      _ <- parseKeyword "INSERT"
      _ <- parseWhitespace
      _ <- parseKeyword "INTO"
      _ <- parseWhitespace
      tableName <- parseName
      _ <- parseWhitespace
      _ <- parseChar '('
      _ <- many parseWhitespace
      columnName1 <- parseName
      columnNames <- many $ do
        _ <- many parseWhitespace
        _ <- parseChar ','
        _ <- many parseWhitespace
        parseName
      _ <- many parseWhitespace
      _ <- parseChar ')'
      _ <- parseWhitespace
      _ <- parseKeyword "VALUES"
      _ <- parseWhitespace
      _ <- parseChar '('
      _ <- many parseWhitespace
      value1 <- parseValue
      values <- many $ do
         _ <- many parseWhitespace
         _ <- parseChar ','
         _ <- many parseWhitespace
         parseValue
      _ <- many parseWhitespace
      _ <- parseChar ')'
      linesOfValues <- many $ do
        _ <- many parseWhitespace
        _ <- parseChar ','
        _ <- many parseWhitespace
        _ <- parseChar '('
        _ <- many parseWhitespace
        value1' <- parseValue
        values' <- many $ do
          _ <- many parseWhitespace
          _ <- parseChar ','
          _ <- many parseWhitespace
          parseValue
        _ <- many parseWhitespace
        _ <- parseChar ')'
        _ <- many parseWhitespace
        return (value1' : values')
      _ <- parseChar ';'
      return $ InsertStatement tableName (columnName1 : columnNames) ((value1 : values) : linesOfValues)

parseInsert :: String -> Either ErrorMessage ParsedStatement
parseInsert input =
  case runParser parseInsertStatement input of
    Right (_, parsedStatement) -> Right parsedStatement
    Left errMsg -> Left errMsg

-- PARSING UPDATE 
--data UpdateStatement = Update TableName [(ColumnName, Value)] (Maybe Condition) deriving (Show, Eq)

parseUpdateStatement :: Parser ParsedStatement
parseUpdateStatement = do
  _ <- parseKeyword "UPDATE"
  _ <- parseWhitespace
  tableName <- parseName
  _ <- parseWhitespace
  _ <- parseKeyword "SET"
  _ <- parseWhitespace
  colName1 <- parseName
  _ <- parseWhitespace
  _ <- parseChar '='
  _ <- parseWhitespace
  value1 <- parseValue
  columnsAndValues <- many $ do
    _ <- many parseWhitespace
    _ <- parseChar ','
    _ <- many parseWhitespace
    colName <- parseName
    _ <- many parseWhitespace
    _ <- parseChar '='
    _ <- many parseWhitespace
    value <- parseValue
    return (colName, value)
  _ <- many parseWhitespace
  whereStatement <- parseWhereStatement
  _ <- many parseWhitespace
  _ <- parseChar ';'
  return $ UpdateStatement tableName ((colName1, value1) : columnsAndValues) whereStatement

parseUpdate :: String -> Either ErrorMessage ParsedStatement
parseUpdate input = do
  (_, parsedStatement) <- runParser parseUpdateStatement input
  return parsedStatement

-- PARSING DELETE

--data DeleteStatement = Delete TableName (Maybe Condition) deriving (Show, Eq)

parseDeleteStatement :: Parser ParsedStatement
parseDeleteStatement = do
  _ <- parseKeyword "DELETE"
  _ <- parseWhitespace
  _ <- parseKeyword "FROM"
  _ <- parseWhitespace
  tableName <- parseName
  whereStatement <- parseWhereStatement
  _ <- many parseWhitespace
  _ <- parseChar ';'
  return $ DeleteStatement tableName whereStatement

parseDelete :: String -> Either ErrorMessage ParsedStatement
parseDelete input =
  case runParser parseDeleteStatement input of
    Right (_, parsedStatement) -> Right parsedStatement
    Left errMsg -> Left errMsg




