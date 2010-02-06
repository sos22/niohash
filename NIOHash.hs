{- Hash table implementation which is internally all imperative and
   IO-driven, but provides a nice pure functional interface. -}
{-# LANGUAGE ScopedTypeVariables #-}
module NIOHash(NIOHash, emptyHash, insertHash, lookupHash,
               deleteHash, Forcable) where

import System.IO.Unsafe
import Data.IORef
import Control.Monad

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

{- You can only change the IORefs in here if the result is a semantic
   no-op, in the sense that nobody outside of this module will be able
   to tell that you've done so. -}

{- nh_nr_updates is always the length of nh_updates -}
data Forcable key => NIOHash key value =
    NIOHash { nh_updates :: IORef [Operation key value],
              nh_nr_updates :: IORef Int,
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
{-# NOINLINE emptyHash #-}
emptyHash hash eq =
    unsafePerformIO $ do u <- newIORef []
                         nu <- newIORef 0
                         return $ NIOHash { nh_updates = u,
                                            nh_nr_updates = nu,
                                            nh_hash = hash,
                                            nh_eq = eq }

{- Can't just rewrite the updates IORef in the old hash table, because
   this is a semantic change.  Create a new one instead. -}
deleteHash :: Forcable a => NIOHash a b -> a -> NIOHash a b
deleteHash base key =
    force key $ unsafePerformIO $
          do oldUpdates <- readIORef $ nh_updates base
             oldNrUpdates <- readIORef $ nh_nr_updates base
             newUpdates <- newIORef $ (OpDelete key):oldUpdates
             newNrUpdates <- newIORef $ oldNrUpdates + 1
             return $ base { nh_updates = newUpdates,
                             nh_nr_updates = newNrUpdates }
insertHash :: Forcable a => NIOHash a b -> a -> b -> NIOHash a b
insertHash base key value =
    force key $ unsafePerformIO $
          do oldUpdates <- readIORef $ nh_updates base
             oldNrUpdates <- readIORef $ nh_nr_updates base
             newUpdates <- newIORef $ (OpInsert key value):oldUpdates
             newNrUpdates <- newIORef $ oldNrUpdates + 1
             return $ base { nh_updates = newUpdates,
                             nh_nr_updates = newNrUpdates }

{- We're allowed to put a new operation at the start of the list by
   rewriting the ioref, because this isn't a semantically visible
   change. -}
lookupHash :: Forcable a => NIOHash a b -> a -> Maybe b
lookupHash base key =
    force key $
    unsafePerformIO $ 
    let eq = nh_eq base
    in do oldUpdates <- readIORef $ nh_updates base
          nrUpdates <- readIORef $ nh_nr_updates base
          let (res, c, upd) =
                  foldr (\(op,c) acc ->
                             case op of
                               OpDelete k -> if k `eq` key
                                             then (Nothing, c, op)
                                             else acc
                               OpInsert k v -> if k `eq` key
                                               then (Just v, c, op)
                                               else acc)
                            (Nothing, nrUpdates, OpDelete key) $
                            zip oldUpdates [1..]
              newUpdates = upd:oldUpdates

          {- If we had to search a long way then prepend something to
             the list so that we don't have to do it again. -}
          when (c > 20) $ do writeIORef (nh_updates base) newUpdates
                             writeIORef (nh_nr_updates base) (nrUpdates + 1)

          return res
