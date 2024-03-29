{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Redundant if" #-}
{-# LANGUAGE InstanceSigs #-}
{-# HLINT ignore "Use lambda-case" #-}

module Lib2 where
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName, database)
import Control.Applicative((<|>), empty, Alternative (some, many))
import Data.Char(isAlphaNum, toLower, isSpace, isDigit, isAlpha)
import Data.List
import Data.Maybe
import Debug.Trace
import Text.Read (Lexeme(String))
--import Text.ParserCombinators.ReadP (optional, sepBy1)
import Control.Monad.Trans.State.Strict (State, StateT, get, put, runState, runStateT)
import Control.Monad.Trans.Except (ExceptT, throwE, runExceptT)
import Control.Monad.Trans.Class(lift)
import Control.Exception (throw)



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

data WhereAtomicStatement = Where (TableName, ColumnName) Operator (Either (TableName, ColumnName) Value) deriving (Show, Eq)

data Condition
  = Comparison WhereAtomicStatement [(LogicalOp, WhereAtomicStatement)] deriving (Show, Eq) -- string aka column

data Columns = All
  | SelectedColumns [(TableName, ColumnName)]
  | Aggregation [(AggregateFunction, String)] deriving (Show, Eq) -- string aka column 

type ColumnName = String

data Order = Order [(TableName, ColumnName, OrderDirection)] deriving (Show, Eq)

data OrderDirection
  = Asc
  | Desc deriving (Show, Eq)

data ParsedStatement
  = ShowTablesStatement
  | ShowTableStatement TableName
  | SelectStatement Columns [TableName] (Maybe Condition) (Maybe Order)
  | InsertStatement TableName [ColumnName] [[Value]]
  | UpdateStatement TableName [(ColumnName, Value)] (Maybe Condition)
  | DeleteStatement TableName (Maybe Condition)
  | CreateTableStatement TableName [(ColumnName, ColumnType)] deriving (Show, Eq)-- Condition

type ParseError = String
type Parser a = ExceptT ParseError (State String) a

runParser :: Parser a -> String -> Either ParseError (String, a)
runParser p input = case runState (runExceptT p) input of
    (Left err, _) -> Left err
    (Right result, remainingInput) -> Right (remainingInput, result)

tryParse :: Parser a -> Parser a -> Parser a
tryParse p1 p2 = do
  result1 <- lift $ runState (runExceptT p1) <$> get
  case result1 of
    (Right _, _) -> p1
    _ -> p2

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
    _ | "createtable" `isPrefixOf` input' -> do
        parsedCreate <- parseCreateTable input
        Right parsedCreate
    _ -> Left "Not implemented: parseStatement"

parseSelectQuery :: String -> Either ErrorMessage ParsedStatement
parseSelectQuery input = do
    (input', columns) <- parseColumnListQuery input
    --(skip, _) <- runParser parseWhitespace input'
    (input'', tableNames) <- parseFromClause input'
    (input''', conditions) <- parseWhere input''
    (input'''', orders) <- parseOrders input'''
    return (SelectStatement columns tableNames conditions orders)

parseFromClause :: String -> Either ErrorMessage (String, [TableName])
parseFromClause input = do
    (rest, _) <- runParser (parseKeyword "FROM") input
    (rest',_) <- runParser parseWhitespace rest
    (rest'', tableNames) <- runParser parseTableNames rest'
    return (rest'', tableNames)

parseTableNames :: Parser [TableName]
parseTableNames = do
  _ <- many parseWhitespace
  tableName <- parseName
  otherTableNames <- many $ do
    _ <- many parseWhitespace
    _ <- parseChar ','
    _ <- many parseWhitespace
    parseName
  return (tableName : otherTableNames)

 -- Executes a parsed statement. Produces a DataFrame. Uses
-- InMemoryTables.databases a source of data.
-- Execute a SELECT statement
executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement (SelectStatement columns tableNames maybeCondition maybeOrder) = do

  -- Fetch the specified tables
  tableDataList <- mapM fetchTableFromDatabase tableNames

  -- Check if aggregation is requested
  let isAggregationRequested = case columns of
        Aggregation _ -> True
        _ -> False

  -- Check if joining tables is requested
  let numberOfTables = length tableNames
  --if numberOfTables 
  let isJoinRequested = case maybeCondition of
        Just condition -> involvesMultipleTables condition
        _ -> False

  -- Perform inner join if requested
  if isJoinRequested then
     if numberOfTables == 2 then executeJoin tableDataList maybeCondition columns isAggregationRequested else Left "only two tables can be joined"
  else
    if numberOfTables /= 1 then Left "only one table should be provided" else executeNoJoin tableDataList maybeCondition columns isAggregationRequested

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
    evaluateWhereStatement (Where (tableName, columnName) op valueEither) row = case op of
        Equals -> case valueEither of
            Right (StringValue stringValue) -> extractValue columnName row == stringValue
        NotEquals -> case valueEither of
            Right (StringValue stringValue) -> extractValue columnName row /= stringValue
        LessThanOrEqual -> case valueEither of
            Right (StringValue stringValue) -> extractValue columnName row <= stringValue
        GreaterThanOrEqual -> case valueEither of
            Right (StringValue stringValue) -> extractValue columnName row >= stringValue

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

-- Function to check if a condition involves multiple tables (MUST BE FIRST)
involvesMultipleTables :: Condition -> Bool
involvesMultipleTables (Comparison whereStatement _) = involvesMultipleTablesInWhere whereStatement -- || any (\(_, cond) -> involvesMultipleTablesInWhere cond) rest --join part must be always the first in where clause
involvesMultipleTables _ = False

-- Function to check if a WHERE clause involves multiple tables
involvesMultipleTablesInWhere :: WhereAtomicStatement -> Bool
involvesMultipleTablesInWhere (Where (tableName1, _) _ (Left (tableName2, _))) = tableName1 /= tableName2
involvesMultipleTablesInWhere _ = False

-- Function to execute an inner join
executeJoin :: [(TableName, DataFrame)] -> Maybe Condition -> Columns -> Bool -> Either ErrorMessage DataFrame
executeJoin tableDataList maybeCondition columns isAggregationRequested = do
    -- Extract tables and condition
    let (table1Name, table1) = head tableDataList
    let (table2Name, table2) = head (tail tableDataList)
    --let joinCondition = fromMaybe (error "Invalid join condition") maybeCondition -- ???

    -- Ensure join tables have the same column
    let (joinColumnNameTable1, joinColumnNameTable2) = case maybeCondition of
            Just (Comparison (Where (tableName1, columnName1) _ (Left (tableName2, columnName2))) _) ->
                if tableName1 == table1Name
                    then (columnName1, columnName2)
                    else (columnName2, columnName1)
            Just _ -> error "Invalid join condition"
            Nothing -> error "Invalid join condition"

    -- Fetch the columns to be selected from the join
    let selectedColumnsTable1 = getColumns table1
    let selectedColumnsTable2 = getColumns table2

    -- Ensure the join columns exist in their respective tables
    joinColumnIndexTable1 <- case findColumnIndex selectedColumnsTable1 joinColumnNameTable1 of
      Just index -> Right index
      Nothing -> Left "Column not found in table1"

    joinColumnIndexTable2 <- case findColumnIndex selectedColumnsTable2 joinColumnNameTable2 of
      Just index -> Right index
      Nothing -> Left "Column not found in table2"

    let resultColumns = case columns of
          All -> selectedColumnsTable1 ++ selectedColumnsTable2
          SelectedColumns colNames ->
            selectedColumnsTable1 ++ filter (\col -> extractColumnName col `elem` map snd colNames) selectedColumnsTable2

    -- Perform the inner join
    let joinedRows = innerJoin joinColumnIndexTable1 joinColumnIndexTable2 table1 table2 maybeCondition

    -- Create a new DataFrame with selected columns and joined rows
    let resultDataFrame = DataFrame resultColumns joinedRows

    -- Perform aggregation if requested
    if isAggregationRequested
        then executeAggregation resultDataFrame joinedRows columns
        else executeSelection resultDataFrame joinedRows columns

-- Function to perform inner join
innerJoin :: Int -> Int -> DataFrame -> DataFrame -> Maybe Condition -> [Row]
innerJoin joinColumnIndexTable1 joinColumnIndexTable2 table1 table2 maybeCondition =
  let rowsTable1 = rowsWithIndixes table1
      rowsTable2 = rowsWithIndixes table2
      matchingRows = filter (\(index1, row1, index2, row2) -> evaluateJoinCondition (index1, row1) (index2, row2)) (joinedRows rowsTable1 rowsTable2) -- leaves only needed columns in a row
  in map (\(_, row1, _, row2) -> createJoinedRow joinColumnIndexTable1 joinColumnIndexTable2 row1 row2) matchingRows
  where
    rowsWithIndixes :: DataFrame -> [(Int, Row)]
    rowsWithIndixes (DataFrame _ rows) = zip [0..] rows

    joinedRows :: [(Int, Row)] -> [(Int, Row)] -> [(Int, Row, Int, Row)]
    joinedRows list1 list2 = [(index1, row1, index2, row2) | (index1, row1) <- list1, (index2, row2) <- list2]

    evaluateJoinCondition :: (Int, Row) -> (Int, Row) -> Bool
    evaluateJoinCondition (index1, row1) (index2, row2) =
      case maybeCondition of
        Just (Comparison whereStatement rest) ->
          let result = evaluateWhereStatement whereStatement (row1 ++ row2)
          in result--trace ("Join condition result: " ++ show result ++ ", row1: " ++ show row1 ++ ", row2: " ++ show row2) result
        Nothing -> trace ("Join condition result (no condition): " ++ show (index1 == index2)) (index1 == index2)

    evaluateWhereStatement :: WhereAtomicStatement -> Row -> Bool
    evaluateWhereStatement (Where (tableName, columnName) op valueEither) row =
      let
        extractValue' colIndex =
          if colIndex < length row
            then row !! colIndex
            else NullValue
        value1 = extractValue' joinColumnIndexTable1
        value2 = extractValue' (length (getColumns table1) + joinColumnIndexTable2) -- extracting value from the second table (num.of col. of 1st table + index in 2nd table)

      in --trace ("Value1: " ++ show value1 ++ ", Value2: " ++ show value2) $
      case op of
        Equals ->
            case valueEither of
              Left (tableName2, columnName2) -> value1 == value2
        _ -> error "Unsupported operator for join condition"

      where
        findColumnIndex :: [Column] -> ColumnName -> Int
        findColumnIndex columns colName =
          case elemIndex colName (map extractColumnName columns) of
            Just index -> index
            Nothing -> error $ "Column not found: " ++ colName

    createJoinedRow :: Int -> Int -> Row -> Row -> Row
    createJoinedRow joinColumnIndexTable1' joinColumnIndexTable2' row1 row2 =
      let prefix1 = take joinColumnIndexTable1' row1
          suffix1 = drop joinColumnIndexTable1' row1
          prefix2 = take joinColumnIndexTable2' row2
          suffix2 = drop joinColumnIndexTable2' row2
      in prefix1 ++ suffix1 ++ prefix2 ++ suffix2

-- Function to execute selection without join
executeNoJoin :: [(TableName, DataFrame)] -> Maybe Condition -> Columns -> Bool -> Either ErrorMessage DataFrame
executeNoJoin tableDataList maybeCondition columns isAggregationRequested = do
  -- Fetch the specified table
  let (tableName, table) = head tableDataList

  -- Filter rows based on the condition
  let filteredRows = filterRows (getColumns table) table maybeCondition

  -- Perform aggregation if requested
  if isAggregationRequested
      then executeAggregation table filteredRows columns
      else executeSelection table filteredRows columns


executeAggregation :: DataFrame -> [Row] -> Columns -> Either ErrorMessage DataFrame
executeAggregation table filteredRows columns = do
  let aggregationFunctions = case columns of
          Aggregation funcs -> funcs
          _ -> []

  let resultRow = map (\(aggFunc, colName) -> executeAggregationFunction aggFunc (findColumnIndex (getColumns table) colName) filteredRows) aggregationFunctions
  return $ DataFrame (createAggregationColumns aggregationFunctions) [resultRow]

executeSelection :: DataFrame -> [Row] -> Columns -> Either ErrorMessage DataFrame
executeSelection table filteredRows columns = do
    let selectedColumnNames = case columns of
            All -> map extractColumnName (getColumns table)
            SelectedColumns colNames -> map snd colNames
    let selectedColumnIndexes = mapMaybe (\colName -> findColumnIndex (getColumns table) colName) selectedColumnNames

    let selectedColumns = map (\i -> (getColumns table) !! i) selectedColumnIndexes

    let selectedRows = map (\row -> map (\i -> (row !! i)) selectedColumnIndexes) filteredRows

    return $ DataFrame selectedColumns selectedRows
------------------------------PARSERS (BEFORE WHERE CLAUSE)----------------------------
parseName :: Parser String
parseName = do
    inp <- lift get
    case takeWhile isAlphaNum inp of --recognizes only letters and numbers (takeWhile stops when the first char which returns false occurses)
        [] -> throwE "Empty input"
        xs -> do
           lift $ put (drop (length xs) inp)
           return xs

parseChar :: Char -> Parser Char
parseChar a = do
    inp <- lift get
    case inp of
        [] -> throwE "Empty input"
        (x:xs) -> if a == x then do
                                lift $ put xs
                                return a
                            else throwE ([a] ++ " expected but " ++ [x] ++ " found")


parseColumns :: Parser Columns
parseColumns = tryParse parseAll (tryParse parseColumnList parseAggregationStatement)

-- parseAll :: Parser Columns
-- parseAll = fmap (\_ -> All) $ parseChar '*'
parseAll :: Parser Columns
parseAll = do
    _ <- many parseWhitespace
    _ <- parseChar '*'
    _ <- many parseWhitespace
    return All

parseColumnList :: Parser Columns
parseColumnList = do
    (tableName, columnName) <- parseTableAndColumnName
    other <- many parseCSName
    return $ SelectedColumns $ (tableName, columnName) : other

parseTableAndColumnName :: Parser (TableName, ColumnName)
parseTableAndColumnName = do
    _ <- many $ parseChar ' '
    tableName <- parseName
    _ <- parseChar '.'
    columnName <- parseName
    return (tableName, columnName)

parseCSName :: Parser (String, String)
parseCSName = do
    _ <- many $ parseChar ' ' --many means that can be zero or more ' ' chars
    _ <- parseChar ','
    _ <- many $ parseChar ' '
    parseTableAndColumnName

parseColumnListQuery :: String -> Either ErrorMessage (String, Columns)
parseColumnListQuery inp = runParser parseColumns inp

parseKeyword :: String -> Parser String
parseKeyword keyword = do
  inp <- lift get
  let l = length keyword
  if map toLower (take l inp) == map toLower keyword &&
    (null (drop l inp) || not (isAlpha (head (drop l inp)))) then
    do
      lift $ put (drop l inp)
      return keyword
  else
    throwE $ keyword ++ " expected"

----------------WHERE PARSER-----------------------
parseLogicalOp :: Parser LogicalOp
parseLogicalOp = parseKeyword "OR" >> pure Or

parseOperator :: Parser Operator
parseOperator = parseEquals <|> parseNotEquals <|> parseGreaterThanOrEqual <|> parseLessThanOrEqual

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
      condition <- parseCondition
      otherConditions <- many $ do
        _ <- parseWhitespace
        logicalOp <- parseLogicalOp
        _ <- parseWhitespace
        condition' <- parseCondition
        return (logicalOp, condition')
      return $ Just $ Comparison condition otherConditions

    parseWithoutWhere = pure Nothing

-- Parser for atomic conditions
parseCondition :: Parser WhereAtomicStatement
parseCondition = tryParse parseComparisonWithStringValue parseComparisonWithColumnReference
  where
    parseComparisonWithStringValue :: Parser WhereAtomicStatement
    parseComparisonWithStringValue = do
      _ <- many parseWhitespace
      (tableName, columnName) <- parseTableAndColumnName
      _ <- many parseWhitespace
      op <- parseOperator
      _ <- many parseWhitespace
      _ <- parseQuotationMarks
      conditionString <- parseName
      _ <- parseQuotationMarks
      return $ Where (tableName, columnName) op (Right (StringValue conditionString))

    parseComparisonWithColumnReference :: Parser WhereAtomicStatement
    parseComparisonWithColumnReference = do
      _ <- many parseWhitespace
      (tableName1, columnName1) <- parseTableAndColumnName
      _ <- many parseWhitespace
      op <- parseEquals
      _ <- many parseWhitespace
      (tableName2, columnName2) <- parseTableAndColumnName
      return $ Where (tableName1, columnName1) op (Left (tableName2, columnName2))


parseWhere :: String -> Either ErrorMessage (String, Maybe Condition)
parseWhere = runParser parseWhereStatement

-----------------------------ORDER PARSER--------------------------------

parseOrders :: String -> Either ErrorMessage (String, Maybe Order)
parseOrders = runParser parseOrderStatement

parseOrderStatement :: Parser (Maybe Order)
parseOrderStatement = parseWithOrder <|> parseWithoutOrder
  where
    parseWithOrder = do
      _ <- many parseWhitespace
      _ <- parseKeyword "ORDER BY"
      _ <- many parseWhitespace
      firstOrderItem <- parseOrderItem
      otherOrderItems <- many $ do
        _ <- parseChar ','
        _ <- many parseWhitespace
        orderItem <- parseOrderItem
        return orderItem
      return $ Just $ Order (firstOrderItem : otherOrderItems)

    parseWithoutOrder = pure Nothing

parseOrderItem :: Parser (TableName, ColumnName, OrderDirection)
parseOrderItem = do
    _ <- many parseWhitespace
    (tableName, columnName) <- parseTableAndColumnName
    _ <- many parseWhitespace
    direction <- parseOrderDirection
    return (tableName, columnName, direction)

parseOrderDirection :: Parser OrderDirection
parseOrderDirection = parseAsc <|> parseDesc
  where
    parseAsc = do
      _ <- many parseWhitespace
      _ <- parseKeyword "ASC"
      return Asc

    parseDesc = do
      _ <- many parseWhitespace
      _ <- parseKeyword "DESC"
      return Desc

-----------MIN SUM----------------------------------------------
parseAggregateFunction :: String -> Either ErrorMessage (String, Columns)
parseAggregateFunction = runParser parseAggregationStatement

parseAggregationStatement :: Parser Columns
parseAggregationStatement = do
    _ <- many $ parseChar ' '
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
parseAggregateFunction' = tryParse parseMin parseSum
  where
    parseMin = parseKeyword "MIN" >> pure Min
    parseSum = parseKeyword "SUM" >> pure Sum

--used in ShowTables execution
showTables :: Database -> [TableName]
showTables = map fst

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
      --parseInteger = Parser $ \inp ->
      parseInteger = do
        inp <- lift get
        let (digits, rest) = span isDigit inp
        case reads digits of
          [(n, "")] -> do
            lift $ put rest
            return n
          _ -> throwE "Error: Not a valid integer"

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
parseUpdate input =
  case runParser parseUpdateStatement input of
    Right (_, parsedStatement) -> Right parsedStatement
    Left errMsg -> Left errMsg

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

parseCreateTableStatement :: Parser ParsedStatement
parseCreateTableStatement = do
  _ <- many parseWhitespace
  _ <- parseKeyword "CREATE"
  _ <- parseWhitespace
  _ <- parseKeyword "TABLE"
  _ <- parseWhitespace
  tableName <- parseName
  _ <- many parseWhitespace
  _ <- parseChar '('
  _ <- many parseWhitespace

  columnName <- parseName
  _ <- many $ parseChar ' '
  columnTypeString <- parseName
  (columnType, size) <- handleOptionalNumber $ map toLower columnTypeString

  other <- many parseCSColNameAndType

  _ <- many parseWhitespace
  _ <- parseChar ')'
  _ <- many parseWhitespace
  _ <- parseChar ';'
  return $ CreateTableStatement tableName ((columnName, columnType) : other)

parseCSColNameAndType :: Parser (ColumnName, ColumnType)
parseCSColNameAndType = do
    _ <- many $ parseChar ' ' --many means that can be zero or more ' ' chars
    _ <- parseChar ','
    _ <- many $ parseChar ' '
    parseColumnNameAndColumnType

parseColumnNameAndColumnType  :: Parser (ColumnName, ColumnType)
parseColumnNameAndColumnType = do
    _ <- many $ parseChar ' '
    columnName <- parseName
    _ <- many $ parseChar ' '
    columnTypeString <- parseName
    (columnType, size) <- handleOptionalNumber $ map toLower (filter (not . isSpace) columnTypeString)
    return (columnName, columnType)

convertToColumnType :: String -> ColumnType
convertToColumnType ct
  | "int" == ct = IntegerType
  | "boolean" == ct = BoolType
  | otherwise = error $ ct ++ "is an unknown column type"

parseCreateTable :: String -> Either ErrorMessage ParsedStatement
parseCreateTable input =
  case runParser parseCreateTableStatement input of
    Right (_, parsedStatement) -> Right parsedStatement
    Left errMsg -> Left errMsg

handleOptionalNumber :: String -> Parser (ColumnType, Maybe String)
handleOptionalNumber "char" = do
  size <- parseSize
  return (StringType, size)
handleOptionalNumber "varchar" = do
  size <- parseSize
  return (StringType, size)
handleOptionalNumber other = return (convertToColumnType other, Nothing)

-- Helper function to parse optional size
parseSize :: Parser (Maybe String)
parseSize = do
  _ <- parseChar '('
  sizeString <- parseName
  let size = takeWhile isDigit sizeString
  _ <- parseChar ')'
  return $ Just size




