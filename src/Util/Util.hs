{-# LANGUAGE ScopedTypeVariables #-}
module Util.Util where

import Control.Monad.Trans
import Control.Monad.Trans.Either


mapLeft :: (a -> c) -> Either a b -> Either c b
mapLeft f (Left x)  = Left (f x)
mapLeft _ (Right x) = Right x

maybeToEither :: e -> Maybe v -> Either e v
maybeToEither _ (Just x) = Right x
maybeToEither e Nothing  = Left e

ioEither :: IO (Either l r) -> EitherT l IO r
ioEither action = do
  result <- liftIO action
  hoistEither result
