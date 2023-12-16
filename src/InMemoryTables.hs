module InMemoryTables
  ( tableEmployees,
    tableInvalid1,
    tableInvalid2,
    tableLongStrings,
    tableWithNulls,
    database,
    TableName,
  )
where

import DataFrame (Column (..), ColumnType (..), DataFrame (..), Value (..))

type TableName = String

tableEmployees :: (TableName, DataFrame)
tableEmployees =
  ( "employees",
    DataFrame
      [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType]
      [ [IntegerValue 1, StringValue "Ed", StringValue "Po"],
        [IntegerValue 2, StringValue "Vi", StringValue "Dl"],
        [IntegerValue 3, StringValue "KN", StringValue "KS"],
        [IntegerValue 4, StringValue "DN", StringValue "DS"],
        [IntegerValue 5, StringValue "AN", StringValue "AS"]
      ]
  )

tableEmployees2 :: (TableName, DataFrame)
tableEmployees2 =
  ( "employees2",
    DataFrame
      [Column "name" StringType, Column "job" StringType]
      [ [StringValue "Vi", StringValue "job1"],
        [StringValue "Ed", StringValue "job2"],
        [StringValue "KN", StringValue "job3"],
        [StringValue "DN", StringValue "job4"],
        [StringValue "AN", StringValue "job5"]
      ]
  )

tableEmployees3 :: (TableName, DataFrame)
tableEmployees3 =
  ( "employees3",
    DataFrame
      [Column "id" IntegerType, Column "job" StringType]
      [ [IntegerValue 1, StringValue "job1"],
        [IntegerValue 2, StringValue "job2"],
        [IntegerValue 3, StringValue "job3"],
        [IntegerValue 4, StringValue "job4"],
        [IntegerValue 5, StringValue "job5"]
      ]
  )
tableInvalid1 :: (TableName, DataFrame)
tableInvalid1 =
  ( "invalid1",
    DataFrame
      [Column "id" IntegerType]
      [ [StringValue "1"]
      ]
  )

tableInvalid2 :: (TableName, DataFrame)
tableInvalid2 =
  ( "invalid2",
    DataFrame
      [Column "id" IntegerType, Column "text" StringType]
      [ [IntegerValue 1, NullValue],
        [IntegerValue 1]
      ]
  )

longString :: Value
longString =
  StringValue $
    unlines
      [ "Lorem ipsum dolor sit amet, mei cu vidisse pertinax repudiandae, pri in velit postulant vituperatoribus.",
        "Est aperiri dolores phaedrum cu, sea dicit evertitur no. No mei euismod dolorem conceptam, ius ne paulo suavitate.",
        "Vim no feugait erroribus neglegentur, cu sed causae aeterno liberavisse,",
        "his facer tantas neglegentur in. Soleat phaedrum pri ad, te velit maiestatis has, sumo erat iriure in mea.",
        "Numquam civibus qui ei, eu has molestiae voluptatibus."
      ]

tableLongStrings :: (TableName, DataFrame)
tableLongStrings =
  ( "long_strings",
    DataFrame
      [Column "text1" StringType, Column "text2" StringType]
      [ [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString],
        [longString, longString]
      ]
  )

tableWithNulls :: (TableName, DataFrame)
tableWithNulls =
  ( "flags",
    DataFrame
      [Column "flag" StringType, Column "value" BoolType]
      [ [StringValue "a", BoolValue True],
        [StringValue "b", BoolValue True],
        [StringValue "b", NullValue],
        [StringValue "b", BoolValue False]
      ]
  )

database :: [(TableName, DataFrame)]
database = [tableEmployees, tableEmployees2,tableEmployees3, tableInvalid1, tableInvalid2, tableLongStrings, tableWithNulls]
