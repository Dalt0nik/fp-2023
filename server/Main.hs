{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Redundant return" #-}
{-# HLINT ignore "Use withFile" #-}
{-# OPTIONS_GHC -Wno-deprecations #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Web.Scotty
import qualified Data.Yaml as Yaml
import qualified Data.ByteString.Lazy as BS
import Data.Text (Text, pack, unpack)
import DataFrame-- (DataFrame, Column (..), ColumnType (..), Value (..), Row)
import InMemoryTables (database, TableName)
import Network.HTTP.Types (notFound404)
import Data.Aeson ( ToJSON, FromJSON )
import Data.ByteString (fromStrict)
import qualified Lib3
import Control.Alternative.Free
import Control.Monad.Free
import Lib2
import System.Directory
import Control.Monad.Cont
import Data.Time
import Control.Exception
import Data.Text.Lazy as TL


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
import System.Directory
import qualified Lib3
import Data.ByteString (any)
import Network.HTTP.Types.Status
import Data.IORef (IORef, newIORef, readIORef, writeIORef, atomicWriteIORef, modifyIORef', atomicModifyIORef')
import System.IO.Unsafe (unsafePerformIO)

import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.STM (TVar, newTVarIO, readTVarIO, atomically, writeTVar)
import Control.Concurrent.STM.TVar (modifyTVar')

instance ToJSON ColumnType
instance ToJSON Column
instance ToJSON DataFrame.Value
instance ToJSON DataFrame


-- Convert DataFrame to YAML ByteString
dataFrameToYaml :: DataFrame -> BS.ByteString
dataFrameToYaml df = Data.ByteString.fromStrict $ Yaml.encode df


getTableRoute :: TableName -> ActionM ()
getTableRoute tableName = do
  let maybeTable = lookup tableName database
  case maybeTable of
    Just table -> raw $ dataFrameToYaml table
    Nothing -> status notFound404


runExecuteIOEndpoint :: String -> IO (Either String DataFrame)
runExecuteIOEndpoint query = do
  result <- runExecuteIO $ Lib3.executeSql query
  case result of
    Right df -> return $ Right df
    Left errMsg -> return $ Left errMsg

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Control.Monad.Free.Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetCurrentTime next) = getCurrentTime >>= return . next
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

        runStep (Lib3.LoadFile tableName next) = do
          db <- readTVarIO inMemoryDb
          case lookup tableName db of
            Just dataFrame -> return $ next (Right dataFrame)
            Nothing -> error ("Table not found: " ++ tableName)

        runStep (Lib3.SaveTable (tableName, dataFrame) next) = do
          atomically $ modifyTVar' inMemoryDb (\db -> (tableName, dataFrame) : Prelude.filter (\(name, _) -> name /= tableName) db)
          return (next ())
        -- atomicModifyIORef' is strict version of atomicModifyIORef; atomicModifyIORef' is threadsafe version of ModifyIORef'

        runStep (Lib3.GetAllTables () f) = do
          tables <- getDirectoryContents "db"
          case Prelude.filter (`notElem` [".", ".."]) tables of
            [] -> return $ f (Left "no tables found")
            _ -> return $ f (Right tables)

----------------------------------------------------------------------
type InMemoryDb = [(TableName, DataFrame)]

inMemoryDb :: TVar InMemoryDb
inMemoryDb = unsafePerformIO (newTVarIO []) --it's a way to create a global mutable variable

initDB :: IO ()
initDB = do
  
  files <- listDirectory "db"
  
  tables <- forM files $ \fileName -> do
    let tableName = Prelude.takeWhile (/= '.') fileName
    let filePath = "db/" ++ fileName
    contentResult <- bracket
        (openFile filePath ReadMode)
        hClose
        (\handle -> do
            fileContent <- hGetContents handle
            evaluate (Prelude.length fileContent)
            return $ either (const defaultDataFrame) id $ Lib3.deserializeDataFrame fileContent
        )
    return (tableName, contentResult)
  atomically $ writeTVar inMemoryDb tables
  where
  defaultDataFrame =
    DataFrame
      [ Column "Error occured" IntegerType
      ]
      []

----------------------------------------------------------------------
app :: ScottyM ()
app = do
  get "/tables/:name" $ do --http://localhost:3000/tables
    tableName <- param "name"
    getTableRoute tableName
  post "/execute" $ do
    requestBody <- body
    case Yaml.decodeEither (BS.toStrict requestBody) of
      Right (query :: String) -> do
        result <- liftIO $ runExecuteIOEndpoint query
        case result of
          Right df -> raw $ dataFrameToYaml df
          Left errMsg -> status badRequest400 >> text (TL.fromStrict $ Data.Text.pack errMsg)
      Left yamlError ->
        status badRequest400 >> text (TL.fromStrict $ Data.Text.pack $ "YAML parsing error: " ++ yamlError)

periodicSave :: IO ()
periodicSave = do
  forever $ do
    threadDelay (10 * 1000000)  -- Delay for 10 seconds
    db <- readTVarIO inMemoryDb
    forM_ db $ \(tableName, dataFrame) -> do
      let filePath = "db/" ++ tableName ++ ".json"
      let jsonStr = Lib3.serializeDataFrame dataFrame
      Prelude.writeFile filePath jsonStr

main :: IO ()
main = do
  initDB
    -- Start the periodicSave function in a separate thread
  _ <- forkIO periodicSave -- returns thread id but we dont care about it
  scotty 3000 app