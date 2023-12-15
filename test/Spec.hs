import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import qualified DataFrame
import Lib1
import qualified Lib2
import qualified Lib3 
import Test.Hspec
import Control.Monad.Trans.Except (runExceptT)
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import Control.Monad.Free (Free (..))


-- mockDatabaseJSON :: String
-- mockDatabaseJSON = "[[ ["id", "IntegerType"], ["name", "StringType"], ["surname", "StringType"] ], [ [{"contents":200,"tag":"IntegerValue"}, {"contents":"ooo","tag":"StringValue"}, {"contents":"tom","tag":"StringValue"}], [{"contents":200,"tag":"IntegerValue"}, {"contents":"ooo","tag":"StringValue"}, {"contents":"hellno","tag":"StringValue"}], [{"contents":100,"tag":"IntegerValue"}, {"contents":"ooo","tag":"StringValue"}, {"contents":"hell","tag":"StringValue"}], [{"contents":69,"tag":"IntegerValue"}, {"contents":"don","tag":"StringValue"}, {"contents":"don","tag":"StringValue"}] ]]"

-- Lib3.deserializeDataFrame mockDatabaseJSON 


main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null
  describe "Lib2.parseStatement:" $ do
    describe "ShowTables" $ do
      it "Parses \"Show Tables\" with mixed-case and multiple spaces" $ do
        let input = "show   TABLES  "
        let expectedOutput = Right Lib2.ShowTablesStatement
        Lib2.parseStatement input `shouldBe` expectedOutput
    describe "ShowTableName" $ do
      it "Parses \"Show Table <name>\" with mixed-case and multiple spaces" $ do
          let input = Lib2.parseStatement "  show taBLe     employees"
          let expectedOutput = Right (Lib2.ShowTableStatement "employees")
          input `shouldBe` expectedOutput
  describe "SelectStatement" $ do
    it "Parses SELECT statement with single column (case insensitive and extra spaces)" $ do
      let input = "SELECT employees.name FROM employees"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.SelectedColumns [("employees","name")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses SELECT statement with single condition" $ do
      let input = "SELECT employees.name FROM employees where employees.name = 'Vi'"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.SelectedColumns [("employees", "name")]) ["employees"] (Just (Lib2.Comparison (Lib2.Where ("employees", "name") Lib2.Equals (Right (DataFrame.StringValue "Vi"))) [])))
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses SELECT statement with multiple conditions" $ do
      let input = "SELECT employees.name FROM employees where employees.name = 'Vi' OR employees.surname = 'AS'"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.SelectedColumns [("employees", "name")]) ["employees"] (Just (Lib2.Comparison (Lib2.Where ("employees", "name") Lib2.Equals (Right (DataFrame.StringValue "Vi"))) [(Lib2.Or, Lib2.Where ("employees", "surname") Lib2.Equals (Right (DataFrame.StringValue "AS")))])))
      Lib2.parseStatement input `shouldBe` expectedOutput    
    it "Parses SELECT statement with aggregation function" $ do
      let input = "SELECT SUM(id) FROM employees"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum,"id")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses SELECT statement with multiple aggregation functions" $ do
      let input = "SELECT SUM(id), MIN(id) FROM employees"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum,"id"),(Lib2.Min,"id")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses SELECT statement with aggregation function and a condition" $ do
      let input = "SELECT SUM(id) FROM employees WHERE employees.name <> 'a'"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum,"id")]) ["employees"] (Just (Lib2.Comparison (Lib2.Where ("employees", "name") Lib2.NotEquals (Right (DataFrame.StringValue "a"))) [])))
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses SELECT statement with aggregation function and multiple conditions" $ do
      let input = "SELECT sum(id) FROM employees WHERE employees.name = 'Vi' OR employees.surname = 'AS'"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum, "id")]) ["employees"] (Just (Lib2.Comparison (Lib2.Where ("employees", "name") Lib2.Equals (Right (DataFrame.StringValue "Vi"))) [(Lib2.Or, Lib2.Where ("employees", "surname") Lib2.Equals (Right (DataFrame.StringValue "AS")))])))
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses SELECT statement with joinded tables" $ do
      let input = "SELECT employees.id, employees3.job FROM employees, employees3 WHERE employees.id = employees3.id"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.SelectedColumns [("employees", "id"), ("employees3", "job")]) ["employees", "employees3"] (Just (Lib2.Comparison (Lib2.Where ("employees", "id") Lib2.Equals (Left ("employees3", "id"))) [])))
      Lib2.parseStatement input `shouldBe` expectedOutput
  
  describe "InsertStatement" $ do 
    it "Parses INSERT statement" $ do
      let input = "INSERT INTO employees (col1,col2) values ('abc',1), ('def', null);"
      let expectedOutput = Right (Lib2.InsertStatement "employees" ["col1","col2"] [[DataFrame.StringValue "abc",DataFrame.IntegerValue 1],[DataFrame.StringValue "def", DataFrame.NullValue]])  
      Lib2.parseStatement input `shouldBe` expectedOutput 
  describe "DeleteStatement" $ do 
    it "Parses DELETE statement" $ do
      let input = "DELETE FROM employees WHERE employees.surname <= 'Dl';"
      let expectedOutput = Right (Lib2.DeleteStatement "employees" (Just (Lib2.Comparison (Lib2.Where ("employees","surname") Lib2.LessThanOrEqual (Right (DataFrame.StringValue "Dl"))) [])))
      Lib2.parseStatement input `shouldBe` expectedOutput      
  describe "UpdateStatement" $ do 
    it "Parses UPDATE statement" $ do
      let input = "UPDATE employees SET name = 'qqq' WHERE employees.surname <= 'Dl';"
      let expectedOutput = Right (Lib2.UpdateStatement "employees" [("name",DataFrame.StringValue "qqq")] (Just (Lib2.Comparison (Lib2.Where ("employees","surname") Lib2.LessThanOrEqual (Right (DataFrame.StringValue "Dl"))) [])))
      Lib2.parseStatement input `shouldBe` expectedOutput



  describe "Helper Functions:" $ do
    describe "fetchTableFromDatabase" $ do
      it "fetches an existing table from the database" $ do
        let result = Lib2.fetchTableFromDatabase "employees"
        result `shouldBe` Right ("employees", snd D.tableEmployees)
    describe "getColumns" $ do
      it "gets columns from a DataFrame" $ do
        let df = snd D.tableEmployees
        Lib2.getColumns df `shouldBe` [DataFrame.Column "id" DataFrame.IntegerType, DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "surname" DataFrame.StringType]
    describe "findColumnIndex" $ do
      it "finds the index of an existing column" $ do
        Lib2.findColumnIndex [DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "age" DataFrame.IntegerType] "name" `shouldBe` Just 0
        Lib2.findColumnIndex [DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "age" DataFrame.IntegerType] "age" `shouldBe` Just 1
  describe "Parsers:" $ do
    describe "parseLogicalOp" $ do
      it "parses OR as LogicalOp" $ do
        let input = "OR"
        Lib2.runParser Lib2.parseLogicalOp input `shouldBe` Right ("", Lib2.Or)
    describe "parseOperator" $ do
      it "parses = as Equals" $ do
          let input = "="
          Lib2.runParser Lib2.parseOperator input `shouldBe` Right ("", Lib2.Equals)
      it "parses <> as NotEquals" $ do
          let input = "<>"
          Lib2.runParser Lib2.parseOperator input `shouldBe` Right ("", Lib2.NotEquals)
      it "parses >= as GreaterThanOrEqual" $ do
          let input = ">="
          Lib2.runParser Lib2.parseOperator input `shouldBe` Right ("", Lib2.GreaterThanOrEqual)
      it "parses <= as LessThanOrEqual" $ do
          let input = "<="
          Lib2.runParser Lib2.parseOperator input `shouldBe` Right ("", Lib2.LessThanOrEqual)
    describe "parseWhitespace" $ do
      it "parses multiple spaces" $ do
        let input = "   "
        Lib2.runParser Lib2.parseWhitespace input `shouldBe` Right ("", "")
    describe "parseQuotationMarks" $ do
      it "parses single quotation mark" $ do
        let input = "'"
        Lib2.runParser Lib2.parseQuotationMarks input `shouldBe` Right ("", "")
    describe "parseWhereStatement" $ do
      it "parses valid WHERE statement with one condition" $ do
        let input = "WHERE column1 = 'value1'"
        Lib2.runParser Lib2.parseWhereStatement input `shouldSatisfy` isRight
      it "parses valid WHERE statement with multiple conditions (OR)" $ do
        let input = "WHERE column1 = 'value1' OR column2 <> 'value2'"
        Lib2.runParser Lib2.parseWhereStatement input `shouldSatisfy` isRight
  describe "Unknown string" $ do
    it "Returns 'Not implemented: parseStatement' for an unknown statement" $ do
      let parsed = Lib2.parseStatement "unknown statement"
      case parsed of
        Left err ->
          if err == "Not implemented: parseStatement"
            then return ()
            else expectationFailure $ "Expected 'Not implemented: parseStatement', but got: " ++ err
        Right _ -> expectationFailure "Expected parsing failure, but returned a valid statement"
  


  
  
  
  
  
  describe "Executes:" $ do
    -- it "Executes TEST_NAME_IN" $ do
    --   let (parsed, rez, expected) =
    --         ( "YOUR SELECT GOES HERE;",
    --           runExecuteIO (Lib3.executeSql parsed),
    --           DataFrame
    --             [ Column "id" IntegerType,
    --               Column "name" StringType,
    --               Column "surname" StringType
    --             ] 
    --             [ [IntegerValue 1, StringValue "Vi", StringValue "Po"], --modify df accordingly
    --               [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
    --               [IntegerValue 3, StringValue "KN", StringValue "KS"],
    --               [IntegerValue 4, StringValue "DN", StringValue "DS"],
    --               [IntegerValue 5, StringValue "AN", StringValue "AS"]
    --             ]
    --         )
    --   result <- rez
    --   result `shouldBe` Right expected
    
    it "Executes simple querries with WHERE statements" $ do
      let (parsed, rez, expected) =
            ( "select * from employees where employees.name <> 'Vi';",
              runExecuteIO (Lib3.executeSql parsed),
              DataFrame
                [ Column "id" IntegerType,
                  Column "name" StringType,
                  Column "surname" StringType
                ]
                [ [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
                  [IntegerValue 3, StringValue "KN", StringValue "KS"],
                  [IntegerValue 4, StringValue "DN", StringValue "DS"],
                  [IntegerValue 5, StringValue "AN", StringValue "AS"]
                ]
            )
      result <- rez
      result `shouldBe` Right expected
    it "Executes INSERT" $ do
      let (parsed, rez, expected) =
            ( "insert into employees (id, name, surname) values (69, 'a','b');",
              runExecuteIO (Lib3.executeSql parsed),
              DataFrame
                [ Column "id" IntegerType,
                  Column "name" StringType,
                  Column "surname" StringType
                ]
                [ [IntegerValue 1, StringValue "Vi", StringValue "Po"],
                  [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
                  [IntegerValue 3, StringValue "KN", StringValue "KS"],
                  [IntegerValue 4, StringValue "DN", StringValue "DS"],
                  [IntegerValue 5, StringValue "AN", StringValue "AS"],
                  [IntegerValue 69, StringValue "a", StringValue "b"]
                ]
            )
      result <- rez
      result `shouldBe` Right expected
    it "Throws an error for insert query with non existing columns" $ do
      let (parsed, rez, expected) =
            ( "insert into employees (notExistingColumn, name, surname) values (69, 'a','b');",
              runExecuteIO (Lib3.executeSql parsed),
              Left "Columns do not exist in the DataFrame"
            )
      result <- rez
      case (expected, result) of
        (Left expectedErr, Left actualErr) -> actualErr `shouldBe` expectedErr
        (Right _, Left _) -> expectationFailure "Expected Left with an error message, but got Right"
        (Left _, Right _) -> expectationFailure "Expected Right, but got Left with an error message"
        (Right _, Right _) -> expectationFailure "Expected Left with an error message, but got Right"
    it "Throws an error for insert query if values in the query mismatch column types" $ do
      let (parsed, rez, expected) =
            ( "insert into employees (id, name, surname) values ('69', 69,'b');",
              runExecuteIO (Lib3.executeSql parsed),
              Left "Invalid values for columns"
            )
      result <- rez
      case (expected, result) of
        (Left expectedErr, Left actualErr) -> actualErr `shouldBe` expectedErr
        (Right _, Left _) -> expectationFailure "Expected Left with an error message, but got Right"
        (Left _, Right _) -> expectationFailure "Expected Right, but got Left with an error message"
        (Right _, Right _) -> expectationFailure "Expected Left with an error message, but got Right"   




    it "Executes UPDATE" $ do
      let (parsed, rez, expected) =
            ( "update employees set name = 'ar' , id = 100 where employees.surname <> 'Dl' ;",
              runExecuteIO (Lib3.executeSql parsed),
              DataFrame
                [ Column "id" IntegerType,
                  Column "name" StringType,
                  Column "surname" StringType
                ]
                [ [IntegerValue 100, StringValue "ar", StringValue "Po"],
                  [IntegerValue 2, StringValue "Ed", StringValue "Dl"],
                  [IntegerValue 100, StringValue "ar", StringValue "KS"],
                  [IntegerValue 100, StringValue "ar", StringValue "DS"],
                  [IntegerValue 100, StringValue "ar", StringValue "AS"]
                ]
            )
      result <- rez
      result `shouldBe` Right expected
    it "Throws an error for update query with non-existing columns" $ do
      let (parsed, rez, expected) =
            ( "update employees set nonExistingColumn = 'newValue' where employees.name = 'Vi';",
              runExecuteIO (Lib3.executeSql parsed),
              Left "Columns to update do not exist in the DataFrame"
            )
      result <- rez
      case (expected, result) of
        (Left expectedErr, Left actualErr) -> actualErr `shouldBe` expectedErr
        (Right _, Left _) -> expectationFailure "Expected Left with an error message, but got Right"
        (Left _, Right _) -> expectationFailure "Expected Right, but got Left with an error message"
        (Right _, Right _) -> expectationFailure "Expected Left with an error message, but got Right"

    it "Throws an error for update query if values in the query mismatch column types" $ do
      let (parsed, rez, expected) =
            ( "update employees set name = 1234 , id = 'a' where employees.name = 'Vi';",
              runExecuteIO (Lib3.executeSql parsed),
              Left "Invalid values for columns"
            )
      result <- rez
      case (expected, result) of
        (Left expectedErr, Left actualErr) -> actualErr `shouldBe` expectedErr
        (Right _, Left _) -> expectationFailure "Expected Left with an error message, but got Right"
        (Left _, Right _) -> expectationFailure "Expected Right, but got Left with an error message"
        (Right _, Right _) -> expectationFailure "Expected Left with an error message, but got Right"       

    it "Executes DELETE" $ do
      let (parsed, rez, expected) =
            ( "delete from employees where employees.surname <> 'Dl';;",
              runExecuteIO (Lib3.executeSql parsed),
              DataFrame
                [ Column "id" IntegerType,
                  Column "name" StringType,
                  Column "surname" StringType
                ]
                [ 
                  [IntegerValue 2, StringValue "Ed", StringValue "Dl"]
                ]
            )
      result <- rez
      result `shouldBe` Right expected  
    it "Executes SHOW TABLE query" $ do
      let (parsed, rez, expected) =
            ( "show table employees;",
              runExecuteIO (Lib3.executeSql parsed),
              DataFrame
                [
                  Column "Column Name" StringType,
                  Column "Column Type" StringType
                ]
                [
                  [StringValue "id", StringValue "IntegerType"],
                  [StringValue "name", StringValue "StringType"],
                  [StringValue "surname", StringValue "StringType"]
                ]
            )
      result <- rez
      result `shouldBe` Right expected          
    it "Executes JOIN" $ do
      let (parsed, rez, expected) =
            ( "SELECT employees.id, employees2.Age FROM employees, employees2 WHERE employees.id = employees2.id;",
              runExecuteIO (Lib3.executeSql parsed),
              Right $ DataFrame
                [ 
                  Column "id" IntegerType,
                  Column "Age" IntegerType
                ] 
                [ 
                  [IntegerValue 1, IntegerValue 25],
                  [IntegerValue 2, IntegerValue 30],
                  [IntegerValue 3, IntegerValue 22],
                  [IntegerValue 4, IntegerValue 20]
                ]
            )
      result <- rez
      result `shouldBe` expected


runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        -- !!!we need to change this
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        -- runStep (Lib3.GetCurrentTime next) = getCurrentTime >>= return . next
        runStep (Lib3.ShowTable tableName f) = do
          tableResult <- runExecuteIO $ Lib3.showTable tableName
          return $ f tableResult

        runStep (Lib3.ParseStatement input next) = do
          let parsedStatement = case Lib2.parseStatement input of
                Right stmt -> stmt
                Left err -> error ("Parsing error: " ++ err)  -- Errors don't work
          return $ next parsedStatement


        runStep (Lib3.ExecuteStatement statement f) = do
          executionResult <- runExecuteIO $ Lib3.executeStatement statement
          case executionResult of
            Right df -> return $ f (Right df)
            Left errMsg -> return $ f (Left errMsg)

        runStep (Lib3.LoadFile tableName next) = case tableName of
          "employees" -> do
            let jsonContent1 =  "[[ [\"id\", \"IntegerType\"], [\"name\", \"StringType\"], [\"surname\", \"StringType\"] ], \
              \[ [{\"contents\":1,\"tag\":\"IntegerValue\"}, {\"contents\":\"Vi\",\"tag\":\"StringValue\"}, {\"contents\":\"Po\",\"tag\":\"StringValue\"}], \
              \[{\"contents\":2,\"tag\":\"IntegerValue\"}, {\"contents\":\"Ed\",\"tag\":\"StringValue\"}, {\"contents\":\"Dl\",\"tag\":\"StringValue\"}], \
              \[{\"contents\":3,\"tag\":\"IntegerValue\"}, {\"contents\":\"KN\",\"tag\":\"StringValue\"}, {\"contents\":\"KS\",\"tag\":\"StringValue\"}], \
              \[{\"contents\":4,\"tag\":\"IntegerValue\"}, {\"contents\":\"DN\",\"tag\":\"StringValue\"}, {\"contents\":\"DS\",\"tag\":\"StringValue\"}], \
              \[{\"contents\":5,\"tag\":\"IntegerValue\"}, {\"contents\":\"AN\",\"tag\":\"StringValue\"}, {\"contents\":\"AS\",\"tag\":\"StringValue\"}] ]]"
            return (next $ Lib3.deserializeDataFrame jsonContent1)
          "employees2" -> do
            -- Handle the myDataFrame2 case
            let jsonContent2 = "[[ [\"id\", \"IntegerType\"], [\"Name\", \"StringType\"], [\"Age\", \"IntegerType\"], [\"IsStudent\", \"BoolType\"] ], \
              \[ [{\"contents\":1,\"tag\":\"IntegerValue\"}, {\"contents\":\"Alice\",\"tag\":\"StringValue\"}, {\"contents\":25,\"tag\":\"IntegerValue\"}, {\"contents\":false,\"tag\":\"BoolValue\"}], \
              \[{\"contents\":2,\"tag\":\"IntegerValue\"}, {\"contents\":\"Bob\",\"tag\":\"StringValue\"}, {\"contents\":30,\"tag\":\"IntegerValue\"}, {\"contents\":true,\"tag\":\"BoolValue\"}], \
              \[{\"contents\":3,\"tag\":\"IntegerValue\"}, {\"contents\":\"Charlie\",\"tag\":\"StringValue\"}, {\"contents\":22,\"tag\":\"IntegerValue\"}, {\"contents\":true,\"tag\":\"BoolValue\"}], \
              \[{\"contents\":4,\"tag\":\"IntegerValue\"}, {\"contents\":\"artiom\",\"tag\":\"StringValue\"}, {\"contents\":20,\"tag\":\"IntegerValue\"}, {\"contents\":true,\"tag\":\"BoolValue\"}] ]]"

            return (next $ Lib3.deserializeDataFrame jsonContent2)   
        -- !!!we need to change this
        runStep (Lib3.SaveTable (tableName, dataFrame) next) = do
          --let filePath = "db/" ++ tableName ++ ".json"
          let jsonStr = Lib3.serializeDataFrame dataFrame
          -- Prelude.writeFile filePath jsonStr --wtf we do with this?
          return (next ())
