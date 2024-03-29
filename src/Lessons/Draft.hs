module Lessons.Draft () where
import Data.Char
type ErrorMessage = String
--type Database = [(TableName, DataFrame)]
type TableName = String
--type DataFrame = -- Define your DataFrame type here

-- Function to parse a "select * from ..." SQL statement and extract a table name
removeSemicolons :: String -> String
removeSemicolons = filter (/= ';')

parseSelectAllStatement' :: String -> Either ErrorMessage TableName
parseSelectAllStatement' sql = case (map toLower sql) of
    ('s':'e':'l':'e':'c':'t':' ':'*':' ':'f':'r':'o':'m':' ':rest) -> 
        case words rest of
            (tableName:_) -> Right (removeSemicolons tableName)
            _ -> Left "Error. Missing table name"
    _ -> Left "Invalid SQL statement: Missing 'SELECT * FROM' statement"

parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement sql = 
    let loweredSql = map toLower sql
    in case words loweredSql of
        ["select", "*", "from", tableName] -> Right tableName
        _ -> Left "Invalid SQL statement: Not a 'SELECT * FROM' statement"




