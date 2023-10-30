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
