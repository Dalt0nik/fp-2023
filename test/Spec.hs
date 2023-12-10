import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import qualified DataFrame
import Lib1
import qualified Lib2
import Test.Hspec



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
    it "Parses \"SELECT employees.name FROM employees\" " $ do
      let input = "SELECT employees.name FROM employees"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.SelectedColumns [("employees","name")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses \"SELECT SUM(id) FROM employees\"" $ do
      let input = "SELECT SUM(id) FROM employees"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum,"id")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses \"SELECT sum(id), min(id) FROM employees\"" $ do
      let input = "SELECT sum(id), min(id) FROM employees"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum,"id"),(Lib2.Min,"id")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses \"SELECT sum(id) FROM employees WHERE name <> 'a'\"" $ do
      let input = "SELECT sum(id) FROM employees WHERE name <> 'a'"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum,"id")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses \"SELECT employees.name FROM employees where name = 'Vi'\"" $ do
      let input = "SELECT employees.name FROM employees where name = 'Vi'"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.SelectedColumns [("employees", "name")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput
    it "Parses \"SELECT employees.name FROM employees where name = 'Vi' OR surname = 'AS'\"" $ do
      let input = "SELECT employees.name FROM employees where name = 'Vi' OR surname = 'AS'"
      let expectedOutput = Right (Lib2.SelectStatement (Lib2.SelectedColumns [("employees", "name")]) ["employees"] Nothing)
      Lib2.parseStatement input `shouldBe` expectedOutput

  describe "ExecuteStatement" $ do
    it "Handles SelectStatement with single column without Condition" $ do
      let result = Lib2.executeStatement (Lib2.SelectStatement (Lib2.SelectedColumns [("employees", "name")]) ["employees"] Nothing)
      let expectedColumns = [DataFrame.Column "name" DataFrame.StringType]
      let expectedRows = [ [DataFrame.StringValue "Ed"]
                          , [DataFrame.StringValue "Vi"]
                          , [DataFrame.StringValue "KN"]
                          , [DataFrame.StringValue "DN"]
                          , [DataFrame.StringValue "AN"]
                          ]
      result `shouldBe` Right (DataFrame.DataFrame expectedColumns expectedRows)
    it "Executes \"SELECT flags.flag FROM flags\" " $ do
      let result = Lib2.executeStatement (Lib2.SelectStatement (Lib2.SelectedColumns [("flags","flag")]) ["flags"] Nothing)
      let expectedColumns = [DataFrame.Column "flag" DataFrame.StringType]
      let expectedRows = [ [DataFrame.StringValue "a"]
                          , [DataFrame.StringValue "b"]
                          , [DataFrame.StringValue "b"]
                          , [DataFrame.StringValue "b"]
                          ]
      result `shouldBe` Right (DataFrame.DataFrame expectedColumns expectedRows)








  describe "Unknown string" $ do
    it "Returns 'Not implemented: parseStatement' for an unknown statement" $ do
      let parsed = Lib2.parseStatement "unknown statement"
      case parsed of
        Left err ->
          if err == "Not implemented: parseStatement"
            then return ()
            else expectationFailure $ "Expected 'Not implemented: parseStatement', but got: " ++ err
        Right _ -> expectationFailure "Expected parsing failure, but returned a valid statement"





------------------------ DIDZIOJI DALIS YRA PERTVARKYTA-----------------  

  -- describe "Lib2.executeStatement:" $ do

  --   describe "ShowTableStatement" $ do
  --     it "Returns expected columns from given table" $ do
  --       let result = Lib2.executeStatement (Lib2.ShowTableStatement "employees")
  --       let expectedColumns = [DataFrame.Column "COLUMN NAMES" DataFrame.StringType]
  --       let expectedRows =[ [DataFrame.StringValue "id"]
  --                         , [DataFrame.StringValue "name"]
  --                         , [DataFrame.StringValue "surname"]
  --                         ]
  --       result `shouldBe` Right (DataFrame.DataFrame expectedColumns expectedRows)
  --   describe "SelectStatement" $ do
  --     it "Handles SelectStatement with single column without Condition" $ do
  --       let result = Lib2.executeStatement (Lib2.SelectStatement (Lib2.SelectedColumns ["name"]) "employees" Nothing)
  --       let expectedColumns = [DataFrame.Column "name" DataFrame.StringType]
  --       let expectedRows = [ [DataFrame.StringValue "Vi"]
  --                         , [DataFrame.StringValue "Ed"]
  --                         , [DataFrame.StringValue "KN"]
  --                         , [DataFrame.StringValue "DN"]
  --                         , [DataFrame.StringValue "AN"]
  --                         ]
  --       result `shouldBe` Right (DataFrame.DataFrame expectedColumns expectedRows)

  --     it "Handles SelectStatement with multiple columns and single Condition" $ do
  --       let condition = Just $ Lib2.Comparison (Lib2.Where "name" Lib2.Equals "Ed") []
  --       let result = Lib2.executeStatement (Lib2.SelectStatement (Lib2.SelectedColumns ["name", "surname"]) "employees" condition)
  --       let expectedColumns = [ DataFrame.Column "name" DataFrame.StringType
  --                             , DataFrame.Column "surname" DataFrame.StringType
  --                             ]
  --       let expectedRows = [[DataFrame.StringValue "Ed", DataFrame.StringValue "Dl"]]
  --       result `shouldBe` Right (DataFrame.DataFrame expectedColumns expectedRows)

  --     it "Handles SelectStatement with single column without Condition" $ do
  --       let inputStatement = Lib2.SelectStatement Lib2.All "employees" (Just (Lib2.Comparison (Lib2.Where "name" Lib2.Equals "Vi") [(Lib2.Or, Lib2.Where "name" Lib2.Equals "KN")]))
  --       let expectedColumns = [DataFrame.Column "id" DataFrame.IntegerType, DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "surname" DataFrame.StringType]
  --       let expectedRows = [ [DataFrame.IntegerValue 1, DataFrame.StringValue "Vi", DataFrame.StringValue "Po"]
  --                           ,[DataFrame.IntegerValue 3, DataFrame.StringValue "KN", DataFrame.StringValue "KS"]
  --                         ]
  --       let expectedOutput = Right (DataFrame.DataFrame expectedColumns expectedRows)
  --       let result = Lib2.executeStatement inputStatement
  --       result `shouldBe` expectedOutput

  --     it "Handles SelectStatement with SUM aggregation" $ do
  --       let inputStatement = Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum, "id")]) "employees" Nothing
  --       let expectedOutput = Right (DataFrame.DataFrame [DataFrame.Column "Sum(id)" DataFrame.IntegerType] [[DataFrame.IntegerValue 15]])
  --       let result = Lib2.executeStatement inputStatement
  --       result `shouldBe` expectedOutput

  --     it "Handles SelectStatement with SUM and MIN aggregations" $ do
  --       let inputStatement = Lib2.SelectStatement (Lib2.Aggregation [(Lib2.Sum, "id"), (Lib2.Min, "id")]) "employees" Nothing
  --       let expectedOutput = Right (DataFrame.DataFrame [DataFrame.Column "Sum(id)" DataFrame.IntegerType, DataFrame.Column "Min(id)" DataFrame.IntegerType] [[DataFrame.IntegerValue 15, DataFrame.IntegerValue 1]])
  --       let result = Lib2.executeStatement inputStatement
  --       result `shouldBe` expectedOutput


  -- describe "Helper Functions:" $ do
  --   describe "fetchTableFromDatabase" $ do
  --     it "fetches an existing table from the database" $ do
  --       let result = Lib2.fetchTableFromDatabase "employees"
  --       result `shouldBe` Right ("employees", snd D.tableEmployees)
  --   describe "getColumns" $ do
  --     it "gets columns from a DataFrame" $ do
  --       let df = snd D.tableEmployees
  --       Lib2.getColumns df `shouldBe` [DataFrame.Column "id" DataFrame.IntegerType, DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "surname" DataFrame.StringType]
  --   describe "findColumnIndex" $ do
  --     it "finds the index of an existing column" $ do
  --       Lib2.findColumnIndex [DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "age" DataFrame.IntegerType] "name" `shouldBe` Just 0
  --       Lib2.findColumnIndex [DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "age" DataFrame.IntegerType] "age" `shouldBe` Just 1


  -- describe "Parsers:" $ do
  --   describe "parseLogicalOp" $ do
  --     it "parses OR as LogicalOp" $ do
  --       let input = "OR"
  --       Lib2.runParser Lib2.parseLogicalOp input `shouldBe` Right ("", Lib2.Or)
  --   describe "parseOperator" $ do
  --     it "parses = as Equals" $ do
  --         let input = "="
  --         Lib2.runParser Lib2.parseOperator input `shouldBe` Right ("", Lib2.Equals)
  --     it "parses <> as NotEquals" $ do
  --         let input = "<>"
  --         Lib2.runParser Lib2.parseOperator input `shouldBe` Right ("", Lib2.NotEquals)
  --     it "parses >= as GreaterThanOrEqual" $ do
  --         let input = ">="
  --         Lib2.runParser Lib2.parseOperator input `shouldBe` Right ("", Lib2.GreaterThanOrEqual)
  --     it "parses <= as LessThanOrEqual" $ do
  --         let input = "<="
  --         Lib2.runParser Lib2.parseOperator input `shouldBe` Right ("", Lib2.LessThanOrEqual)
  --   describe "parseWhitespace" $ do
  --     it "parses multiple spaces" $ do
  --       let input = "   "
  --       Lib2.runParser Lib2.parseWhitespace input `shouldBe` Right ("", "")
  --   describe "parseQuotationMarks" $ do
  --     it "parses single quotation mark" $ do
  --       let input = "'"
  --       Lib2.runParser Lib2.parseQuotationMarks input `shouldBe` Right ("", "")
  --   describe "parseWhereStatement" $ do
  --     it "parses valid WHERE statement with one condition" $ do
  --       let input = "WHERE column1 = 'value1'"
  --       Lib2.runParser Lib2.parseWhereStatement input `shouldSatisfy` isRight

  --     it "parses valid WHERE statement with multiple conditions (OR)" $ do
  --       let input = "WHERE column1 = 'value1' OR column2 <> 'value2'"
  --       Lib2.runParser Lib2.parseWhereStatement input `shouldSatisfy` isRight
  




    -- it "Parses \"SELECT employees.name FROM employees\"" $ do
    --   let result = Lib2.executeStatement (Lib2.SelectStatement (Lib2.SelectedColumns ["employees.name"]) "employees" Nothing) D.database
    --   let expectedColumns = [DataFrame.Column "employees.name" DataFrame.StringType]
    --   let expectedRows = [ [DataFrame.StringValue "Ed"]
    --                     , [DataFrame.StringValue "Vi"]
    --                     , [DataFrame.StringValue "KN"]
    --                     , [DataFrame.StringValue "DN"]
    --                     , [DataFrame.StringValue "AN"]
    --                     ]
    --   result `shouldBe` Right (DataFrame.DataFrame expectedColumns expectedRows)

  --     it "Handles SelectStatement with multiple columns and single Condition" $ do
  --       let condition = Just $ Lib2.Comparison (Lib2.Where "name" Lib2.Equals (DataFrame.StringValue "Ed")) []
  --       let result = Lib2.executeStatement (Lib2.SelectStatement (Lib2.SelectedColumns ["name", "surname"]) "employees" condition) D.database
  --       let expectedColumns = [DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "surname" DataFrame.StringType]
  --       let expectedRows = [[DataFrame.StringValue "Ed", DataFrame.StringValue "Dl"]]
  --       result `shouldBe` Right (DataFrame.DataFrame expectedColumns expectedRows)

  --     it "Handles SelectStatement with single column without Condition" $ do
  --       let inputStatement = Lib2.SelectStatement Lib2.All "employees" (Just (Lib2.Comparison (Lib2.Where "name" Lib2.Equals (DataFrame.StringValue "Vi")) [(Lib2.Or, Lib2.Where "name" Lib2.Equals (DataFrame.StringValue "KN"))]))
  --       let expectedColumns = [DataFrame.Column "id" DataFrame.IntegerType, DataFrame.Column "name" DataFrame.StringType, DataFrame.Column "surname" DataFrame.StringType]
  --       let expectedRows = [ [DataFrame.IntegerValue 1, DataFrame.StringValue "Vi", DataFrame.StringValue "Po"]
  --                         ,[DataFrame.IntegerValue 3, DataFrame.StringValue "KN", DataFrame.StringValue "KS"]
  --                       ]
  --       let expectedOutput = Right (DataFrame.DataFrame expectedColumns expectedRows)
  --       let result = Lib2.executeStatement inputStatement D.database
  --       result `shouldBe` expectedOutput

