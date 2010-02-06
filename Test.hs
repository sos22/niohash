{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Test.QuickCheck
import Control.Monad

import NIOHash

myhash :: NIOHash Int String
myhash = emptyHash fromIntegral

data Operation a b = OperationDelete a
                   | OperationInsert a b deriving Show
                   
instance (Arbitrary a, Arbitrary b) => Arbitrary (Operation a b) where
    arbitrary = oneof [liftM OperationDelete arbitrary,
                       do k <- arbitrary
                          v <- arbitrary
                          return $ OperationInsert k v]

instance Arbitrary Char where
    arbitrary = choose ('a', 'z')

applyOp :: (Eq a, Forcable a) => Operation a b -> NIOHash a b -> NIOHash a b
applyOp (OperationDelete a) base = deleteHash base a
applyOp (OperationInsert a b) base = insertHash base a b

applyOpSeq :: (Eq a, Forcable a) => [Operation a b] -> NIOHash a b -> NIOHash a b
applyOpSeq ops base = foldr applyOp base ops

doesntMentionKey :: Eq a => [Operation a b] -> a -> Bool
doesntMentionKey [] _ = True
doesntMentionKey ((OperationDelete k1):r) k2 | k1 == k2 = False
                                             | otherwise = doesntMentionKey r k2
doesntMentionKey ((OperationInsert k1 _):r) k2
                 | k1 == k2 = False
                 | otherwise = doesntMentionKey r k2

main :: IO ()
main = do quickCheck (\s -> lookupHash myhash s == Nothing)
          quickCheck (\z k v ->
                          lookupHash (insertHash (applyOpSeq z myhash) k v) k == Just v)
          quickCheck (\z k (v::String) ->
                          lookupHash (deleteHash (applyOpSeq z myhash) k) k == Nothing)
          quickCheck (\w z k ->
                      doesntMentionKey w k ==>
                      lookupHash (applyOpSeq w $ applyOpSeq z myhash) k ==
                      lookupHash (applyOpSeq z myhash) k)


