{- Simple wrapper which takes a function f and turns it into a
   function f' which behaves exactly the same, but caches the last N
   evaluations.  The lookup is moderately expensive, so this only
   really makes sense if f is expensive. -}

module SemiMemoise(semimemoise) where

import Data.IORef
import System.IO.Unsafe

import Forcable

data SemiMemo a b = SemiMemo { sm_underlying :: a -> b,
                               sm_nr_memo :: Int,
                               sm_memotab :: IORef [(a, b)] }

mkSemiMemoise :: Int -> (a -> b) -> IO (SemiMemo a b)
mkSemiMemoise nr f = do r <- newIORef []
                        return $ SemiMemo { sm_underlying = f,
                                            sm_nr_memo = nr,
                                            sm_memotab = r }

semiMemoiseLookup :: Eq a => SemiMemo a b -> a -> IO b
semiMemoiseLookup sm key =
    atomicModifyIORef (sm_memotab sm) $ \memotab ->
        let res = case lookup key memotab of
                    Just r -> r
                    Nothing -> sm_underlying sm key
            newtab = take (sm_nr_memo sm) $ (key, res):memotab
        in (newtab, res)

semimemoise :: (Forcable a, Eq a) => Int -> (a -> b) -> (a -> b)
semimemoise nr f =
    unsafePerformIO $
    do n <- mkSemiMemoise nr f
       return $ \x -> force x $ unsafePerformIO $ semiMemoiseLookup n x
