{- Hash table implementation which is internally all imperative and
   IO-driven, but provides a nice pure functional interface. -}

module NIOHash(NIOHash, emptyHash, insertHash, lookupHash,
               deleteHash, Forcable) where

import System.IO.Unsafe
import Data.IORef

class Forcable a where
    force :: a -> b -> b

instance Forcable Int where
    force = seq

data Operation key value = OpDelete key
                         | OpInsert key value

{- The hash table consists of an IO hash table, which is immutable,
   plus a queue of updates to the table.  When the queue gets too big,
   we duplicate the table and commit the updates to the new one.  This
   should give you most of the benefits of a fast hashtable with a
   pure interface. -}
data Forcable key => NIOHash key value =
    NIOHash { nh_updates :: [Operation key value],
              nh_hash :: key -> Int,
              nh_eq :: key -> key -> Bool
            }

{- Equations governing the hash table:

   lookupHash (emptyHash _ _) _ = Nothing

   lookupHash (insertHash (Z $ emptyHash h eq) k1 v) k2 = Just v
   lookupHash (deleteHash (Z $ emptyHash h eq) k1) k2 = Nothing
   when h k1 == h k2 and k1 `eq` k2 == True
   
   lookupHash (W X) k = lookupHash X k

   where:

   -- Z is some sequence of insert and delete operations
   -- X is Z $ emptyHash h eq
   -- W is some sequence of insert and delete operations on keys k2
      such that k `eq` k2 == False.
-}
emptyHash :: Forcable a => (a -> Int) -> (a -> a -> Bool) ->  NIOHash a b
emptyHash hash eq =
    NIOHash { nh_updates = [],
              nh_hash = hash,
              nh_eq = eq }

deleteHash :: Forcable a => NIOHash a b -> a -> NIOHash a b
deleteHash base key =
    force key $ base { nh_updates = (OpDelete key):(nh_updates base) }

insertHash :: Forcable a => NIOHash a b -> a -> b -> NIOHash a b
insertHash base key value =
    force key $ base { nh_updates = (OpInsert key value):(nh_updates base) }

lookupHash :: Forcable a => NIOHash a b -> a -> Maybe b
lookupHash base key =
    let eq = nh_eq base
    in foldl (\acc op ->
                  case op of
                    OpDelete k -> if k `eq` key
                                  then Nothing
                                  else acc
                    OpInsert k v -> if k `eq` key
                                    then Just v
                                    else acc) Nothing $ nh_updates base
