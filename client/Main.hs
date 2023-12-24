{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
module Main (main) where

import Network.Wreq
import Data.Text
import Data.Aeson (ToJSON, FromJSON, toJSON, parseJSON, withObject, (.:))
import GHC.Generics (Generic)
import Control.Lens
import qualified Data.Text.IO as T


data TranslateRequest = TranslateRequest{
    q:: Text,
    source::Text,
    target::Text,
    format::Text,
    api_key:: Text
} deriving(Generic)

instance ToJSON TranslateRequest


data TranslateResponse = TranslateResponse {
    translatedText :: Text
} deriving(Generic)

instance FromJSON TranslateResponse where
    parseJSON = withObject "TranslateResponse" $ \v ->
        TranslateResponse <$> v .: "translatedText"
        
main :: IO ()
main = do
    rsp <- asJSON  =<< post "https://translate.terraprint.co/translate" (toJSON $ TranslateRequest {
        q = "i need a dollar dollar,a dollar is what i need",
		source = "en",
		target = "ja",
		format = "text",
		api_key = ""
    })

    T.putStrLn $ translatedText $ rsp ^. responseBody 
