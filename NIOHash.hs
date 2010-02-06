{- Hash table implementation which is internally all imperative and
   IO-driven, but provides a nice pure functional interface. -}
{-# LANGUAGE ScopedTypeVariables #-}
module NIOHash(NIOHash, emptyHash, insertHash, lookupHash,
               deleteHash, Forcable) where

import qualified Data.HashTable as HT
import System.IO.Unsafe
import Data.IORef
import Control.Monad
import Data.Int

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
data (Eq key, Forcable key) => NIOHash key value =
    NIOHash { nh_updates :: IORef [Operation key value],
              nh_nr_updates :: IORef Int,
              nh_table :: IORef (HT.HashTable key value),
              nh_hash :: key -> Int32
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

emptyHash :: (Eq a, Forcable a) => (a -> Int32) -> NIOHash a b
{-# NOINLINE emptyHash #-}
emptyHash hash =
    unsafePerformIO $ do u <- newIORef []
                         nu <- newIORef 0
                         h <- HT.new (==) hash
                         h' <- newIORef h
                         return $ NIOHash { nh_updates = u,
                                            nh_nr_updates = nu,
                                            nh_table = h',
                                            nh_hash = hash }

{- Can't just rewrite the updates IORef in the old hash table, because
   this is a semantic change.  Create a new one instead. -}
deleteHash :: (Eq a, Forcable a) => NIOHash a b -> a -> NIOHash a b
deleteHash base key =
    force key $ unsafePerformIO $
          do oldUpdates <- readIORef $ nh_updates base
             oldNrUpdates <- readIORef $ nh_nr_updates base
             newUpdates <- newIORef $ (OpDelete key):oldUpdates
             newNrUpdates <- newIORef $ oldNrUpdates + 1
             return $ base { nh_updates = newUpdates,
                             nh_nr_updates = newNrUpdates }
insertHash :: (Eq a, Forcable a) => NIOHash a b -> a -> b -> NIOHash a b
insertHash base key value =
    force key $ unsafePerformIO $
          do oldUpdates <- readIORef $ nh_updates base
             oldNrUpdates <- readIORef $ nh_nr_updates base
             newUpdates <- newIORef $ (OpInsert key value):oldUpdates
             newNrUpdates <- newIORef $ oldNrUpdates + 1
             return $ base { nh_updates = newUpdates,
                             nh_nr_updates = newNrUpdates }

flushUpdateQueue :: (Eq a, Forcable a) => NIOHash a b -> IO ()
flushUpdateQueue base =
    let applyUpdate ht (OpDelete k) = HT.delete ht k
        applyUpdate ht (OpInsert k v) = HT.insert ht k v
    in
    do old_table <- readIORef $ nh_table base
       old_table_list <- HT.toList old_table
       new_table <- HT.fromList (nh_hash base) old_table_list
       writeIORef (nh_nr_updates base) 0
       writeIORef (nh_table base) new_table
       updates <- readIORef $ nh_updates base
       mapM_ (applyUpdate new_table) $ reverse updates

{- We're allowed to put a new operation at the start of the list by
   rewriting the ioref, because this isn't a semantically visible
   change. -}
lookupHash :: (Eq a, Forcable a) => NIOHash a b -> a -> Maybe b
lookupHash base key =
    force key $ {- force the key early, so that we don't need to evaluate
                   externally-provided thunks in the unsafePerformIO block,
                   which can lead to unexpected recursion. -}
    unsafePerformIO $ 
    do oldUpdates <- readIORef $ nh_updates base
       nrUpdates <- readIORef $ nh_nr_updates base
       let (res, c, upd) =
               foldr (\(op,c) acc ->
                          case op of
                            OpDelete k -> if k == key
                                          then (Just Nothing, c, op)
                                          else acc
                            OpInsert k v -> if k == key
                                            then (Just $ Just v, c, op)
                                            else acc)
                         (Nothing, nrUpdates, OpDelete key) $
                         zip oldUpdates [1..]
           newUpdates = upd:oldUpdates
       res' <- case res of
                 Just x -> return x
                 Nothing -> do h <- readIORef $ nh_table base
                               HT.lookup h key
       if c > 50
        then flushUpdateQueue base
        else if c > 20
             then {- If we had to search a long way then prepend
                     something to the list so that we don't have to do
                     it again. -}
                 do writeIORef (nh_updates base) newUpdates
                    writeIORef (nh_nr_updates base) (nrUpdates + 1)
             else return ()
       return res'
