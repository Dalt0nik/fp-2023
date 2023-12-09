module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import Control.Monad (when)

import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import System.IO (withFile, IOMode(ReadMode, WriteMode), hGetContents, hPutStrLn, openFile, hClose)
import Control.Exception (bracket, evaluate)
import Lib1 qualified
import Lib2 qualified
import Lib3 qualified
import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)
import GHC.Real (underflowError)
import qualified Lib3

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      df <- runExecuteIO $ Lib3.executeSql c 
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        -- probably you will want to extend the interpreter
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetCurrentTime next) = getCurrentTime >>= return . next
        runStep (Lib3.ShowTable tableName f) = do
          tableResult <- runExecuteIO $ Lib3.showTable tableName
          return $ f tableResult

        runStep (Lib3.ExecuteInsert statement f) = do --i think this is redundant
          insertResult <- runExecuteIO $ Lib3.executeInsert statement
          case insertResult of
            Right df -> return $ f (Right df)
            Left errMsg -> return $ f (Left errMsg)

        runStep (Lib3.ParseStatement input next) = do
          let parsedStatement = case Lib2.parseStatement input of
                Right stmt -> stmt
                Left err -> error ("Parsing error: " ++ err)  -- Errors don't work
          return $ next parsedStatement

        runStep (Lib3.ExecuteStatement statement next) = do
          let executionResult = Lib3.executeStatement statement
          return $ next executionResult


        -- OLD WAY
        -- runStep (Lib3.LoadFile tableName next) = do
        --   let filePath = "db/" ++ tableName ++ ".json"
        --   fileContent <- Prelude.readFile filePath 
        --   return $ next $ Lib3.deserializeDataFrame fileContent

        runStep (Lib3.LoadFile tableName next) = do
          let filePath = "db/" ++ tableName ++ ".json"
          contentResult <- bracket
              (openFile filePath ReadMode)
              hClose
              (\handle -> do
                  fileContent <- hGetContents handle
                  evaluate (length fileContent)
                  return $ next $ Lib3.deserializeDataFrame fileContent
              )
          return contentResult


        runStep (Lib3.SaveTable (tableName, dataFrame) next) = do
          let filePath = "db/" ++ tableName ++ ".json"
          let jsonStr = Lib3.serializeDataFrame dataFrame
          Prelude.writeFile filePath jsonStr
          return (next ())



