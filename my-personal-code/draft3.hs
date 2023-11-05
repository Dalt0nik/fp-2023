type ErrorMessage = String
type TableName = String
type DataFrame = [(String, String)]  -- For simplicity, assuming a DataFrame is a list of (column name, value) pairs
type Database = [(TableName, DataFrame)]

data ParsedStatement = SelectStatement [String] String (Maybe ParsedStatement) 
--"SELECT" then [string] cols names, then word "FROM"  string tablename then other part of statement(if there is)
                    | MinStatement String (Maybe ParsedStatement)
                    | SumStatement String (Maybe ParsedStatement)
                    | OrStatement [ParsedStatement]
                    | StringComparison String String String
                    | ErrorStatement ErrorMessage

-- SELECT col1 col2 FROM table1 WHERE col1 > 5 OR col2 < 10;
-- SELECT MIN col1 FROM table2 WHERE col1 > 5;
-- SELECT SUM col2 FROM table1 WHERE col2 > 5;

-- Helper function to split a string by spaces
splitBySpace :: String -> [String]
splitBySpace = words

-- Parser for SELECT statement
parseSelectStatement :: [String] -> Either ErrorMessage ParsedStatement
parseSelectStatement ("SELECT" : cols) = Right (SelectStatement cols)
parseSelectStatement _ = Left "Invalid SELECT statement"

-- Parser for COLS statement
parseColumnsStatement :: [String] -> Either ErrorMessage ParsedStatement
parseColumnsStatement ("COLS" : cols) = Right (ColumnsStatement cols)
parseColumnsStatement _ = Left "Invalid COLS statement"

-- Parser for MIN statement
parseMinStatement :: [String] -> Either ErrorMessage ParsedStatement
parseMinStatement ("MIN" : colName : []) = Right (MinStatement colName)
parseMinStatement _ = Left "Invalid MIN statement"

-- Parser for SUM statement
parseSumStatement :: [String] -> Either ErrorMessage ParsedStatement
parseSumStatement ("SUM" : colName : []) = Right (SumStatement colName)
parseSumStatement _ = Left "Invalid SUM statement"

-- Parser for OR statement
parseOrStatement :: [String] -> Either ErrorMessage ParsedStatement
parseOrStatement ("OR" : rest) = parseOrStatements rest []
  where
    parseOrStatements [] subStatements = Right (OrStatement subStatements)
    parseOrStatements ("OR" : remaining) subStatements = parseOrStatements remaining subStatements
    parseOrStatements other subStatements =
      case parseStatement (unwords other) of
        Right stmt -> Right (OrStatement (subStatements ++ [stmt]))
        Left err -> Left err

-- Parser for string comparisons
parseStringComparison :: [String] -> Either ErrorMessage ParsedStatement
parseStringComparison (col1 : op : col2 : []) = Right (StringComparison col1 op col2)
parseStringComparison _ = Left "Invalid string comparison"

-- Parses user input into a ParsedStatement

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement input =
  let tokens = splitBySpace input
  in case tokens of
    ("SHOW TABLES") -> 
    ("SELECT" : _) -> parseSelectStatement tokens
    ("COLS" : _) -> parseColumnsStatement tokens
    ("MIN" : _) -> parseMinStatement tokens
    ("SUM" : _) -> parseSumStatement tokens
    ("OR" : _) -> parseOrStatement tokens
    _ -> parseStringComparison tokens

--SELECT col1 col2 WHERE col1 > MIN col2 
instance Show ParsedStatement where
    show (SelectStatement cols) = "SELECT " ++ unwords cols
    show (ColumnsStatement cols) = "COLS " ++ unwords cols
    show (MinStatement colName) = "MIN " ++ colName
    show (SumStatement colName) = "SUM " ++ colName
    show (OrStatement subStatements) = "OR " ++ unwords (map show subStatements)
    show (StringComparison col1 op col2) = col1 ++ " " ++ op ++ " " ++ col2
    show (ErrorStatement errMsg) = "Error: " ++ errMsg


main :: IO ()
main = do
    let input = "SELECT column1 column2"
    case parseStatement input of
        Left err -> putStrLn $ "Error: " ++ err
        Right statement -> print statement
