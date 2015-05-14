{-# LANGUAGE ScopedTypeVariables #-}
module Util where

import Control.Monad.Trans
import Control.Monad.Trans.Either


-- mapLeft :: (a -> c) -> Either a b -> Either c b
-- mapLeft f (Left x)  = Left (f x)
-- mapLeft _ (Right x) = Right x

maybeToEither :: e -> Maybe v -> Either e v
maybeToEither _ (Just x) = Right x
maybeToEither e Nothing  = Left e

maybeToEitherT :: Monad m => e -> Maybe a -> EitherT e m a
maybeToEitherT x y = hoistEither $ maybeToEither x y

ioEither :: (Monad m) => m (Either l r) -> EitherT l m r
ioEither action = do
  result <- lift action
  hoistEither result
