{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-deprecations #-}
module Main (main) where

import qualified Network.Wreq as Wreq

import Data.ByteString.Lazy (ByteString)
import qualified Data.Yaml as Yaml

import Control.Monad.IO.Class (MonadIO (liftIO))

import Data.List qualified as L

import Lib1 qualified

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
import GHC.TypeError (ErrorMessage)
import Control.Lens.Lens
import Control.Lens.Operators
import Data.ByteString
import Network.HTTP.Client
import Network.HTTP.Client.TLS (tlsManagerSettings)
import Network.HTTP.Types.Status (status200)
import qualified Data.ByteString.Lazy.Char8 as L8
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Encoding as TLE
import qualified Data.Yaml as Yaml
import DataFrame

import Control.Exception

data ExitNow = ExitNow deriving Show

instance Exception ExitNow

runExecuteIOFromServer :: String -> IO (Either String DataFrame)
runExecuteIOFromServer query = do
    manager <- newManager tlsManagerSettings
    let request = parseRequest_ "http://localhost:3000/execute"
    let request' = request { method = "POST"
                           , requestBody = RequestBodyLBS (L8.pack query)
                           , requestHeaders = [("Content-Type", "application/x-yaml")]
                           }
    response <- httpLbs request' manager

    let status = responseStatus response
    if status == status200
        then do
            let body = responseBody response
            case Yaml.decodeEither' (L8.toStrict body) of
                Left err -> return $ Left (show err)
                Right df -> return $ Right df
        else do
            let err = T.unpack . TL.toStrict . TLE.decodeUtf8 $ responseBody response
            return $ Left err



type Repl a = HaskelineT IO a

final :: Repl ExitDecision --The final function is not actually used in our setup.
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to X-Mas database! Press [TAB] for auto completion. Enter ':q' to exit"

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
cmd userInput = do
  if userInput == ":q"
    then liftIO $ throwIO ExitNow
    else do
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
      result <- runExecuteIOFromServer userInput
      case result of
        Right df -> return $ Right (Lib1.renderDataFrameAsTable s df)
        Left errMsg -> return $ Left errMsg

main :: IO ()
main = catch (evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final) 
              (\ExitNow -> putStrLn "Ho-ho-ho, goodbye, son!")


