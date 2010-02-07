module Forcable(Forcable(force)) where

class Forcable a where
    force :: a -> b -> b

instance Forcable Int where
    force = seq

