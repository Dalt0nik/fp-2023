# fp-2023

## Setup
1. Checkout the repository. This project uses GitHub Actions haskell workflow,
please preserve its configuration.
2. Now you have two options
  - Use GitHub Codespaces (Code -> Codespaces) to develop directly in browser. This is paid
  GitHub feature, but: you get a few compute hours for free and you can get even more if you
  register as student.
  - Use your computer:
    - Install [ghcup](https://www.haskell.org/ghcup/), please note you might need to install
      additional packages, as descriped [here](https://www.haskell.org/ghcup/install/). Just agree
      with all defaults during the installation. `ghcup` binary should appear in your `PATH` (you
      might need to restart your computer).
    - Install (if not already installed) VSCode. When done, add Haskell ("Haskell language support")
      extension.
3. Open any .hs file in the checked out (step 1) repository. Haskell extension should pick up
[project settings](.vscode/settings.json) and install all dependencies. This might take some
time. If the magic does not happen, please install ghcup components manually:

```
ghcup install stack --set 2.9.3
ghcup install hls --set 2.0.0.1
ghcup install cabal --set 3.6.2.0
ghcup install ghc --set 9.4.5
```

# Task 1

Please edit [Lib1](src/Lib1.hs) module (only!).

Run your application: `stack run fp2023-select-all`

Run tests: `stack test`

# Task 2

Please edit [Lib2](src/Lib2.hs) module (only!).

Run your application: `stack run fp2023-select-more`

Add more and run tests: `stack test`

### Our requirements for SELECT:

 - column list:
   - Parse and recognize column names in a given query.
   - Return the specified columns from the table in the result.
   - Ensure provided column names exist in the table.
 - min:
   - Parse the MIN aggregate function.
   - Return the smallest value in the specified column.
   - Ensure that only integers, bools, and strings are processed.
 - sum:
   - Parse the SUM aggregate function.
   - Return the sum of all values in the specified column.
   - Ensure that integers are processed.
 - where OR:
   - Parse multiple conditions combined using OR.
   - Include rows in the result if any of the conditions combined with OR are met.
   - Aggregate functions can be applied to the results ( MIN, MAX, etc.)
 - where str =/<>/<=/>=:
   - Parse conditions for strings.
   - Implement string comparisons (=, <> or !=, <=, >=).

# Task 3

Please edit [Lib3](src/Lib3.hs) and [Main](app3/Main.hs) modules. You can add libraries to [package.yaml](package.yaml).

Run your application: `stack run fp2023-manipulate`

Add more and run tests: `stack test`

### Requirements:

 - You have to support "SELECT" statement from multiple tables, so tables might be joined in WHERE clause
 - Support "NOW()" function which return current time.
 - You have to support "UPDATE", "INSERT" and "DELETE" statements which mutate tables data - your tables become writable!
 - You have to read/save tables data from/in file system. You will be informed about your data serializations format when you pass Task2. But you can always ask by dropping an email.
 - Please use "db" directory to store data files. You have to implement data serialization (write) by yourselves. You have to use an already existing haskell library to parse (read) serialized data. You can have a single file in "db" directory, you can have file-per-table - you choose.
 - You do not create new tables in this task, so "CREATE" statements is not needed. You can ship a prepopulated "db" directory with all metada (table names, column names, column types) in data files.
 - You have to implement query execution business logics in DSL based on Free Monad. You have to implement two interpreters: a) "production" one, which reads files from file system and b) "test" one which you use in tests and keeps all data in memory.
 - Reuse Lib1 and Lib2 as much as you can. 