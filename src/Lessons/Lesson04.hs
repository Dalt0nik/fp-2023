module Lessons.Lesson04 () where
search :: Eq a =>[(a, b)] -> a -> Maybe b
search [] _ = Nothing
search ((k,v):xs) key =
    if key == k then Just v else search xs key

data Train = Train Int

instance Eq Train where
    (Train intA) == (Train intB) = intA == intB

instance Mazdaug Train where
    (Train intA) ~=~ (Train intB) = intA ~=~ intB

instance Mazdaug Int where
    a ~=~ b = abs(a - b) < 1

class Mazdaug a where
    (~=~) :: a -> a -> Bool

mazdaugSearch :: Eq a =>[(a, b)] -> a -> Maybe b
mazdaugSearch [] _ = Nothing
mazdaugSearch ((k,v):xs) key =
    if key == k then Just v else mazdaugSearch xs key


dn1 :: [(Integer, Integer)]
dn1 = do
    a <- [1 .. 5]
    b <- [a .. 9]
    return (a, b)

dn2 :: [(Integer, Integer)]
dn2 = do
    a <- [1 .. 5]
    b <- []
    return (a,b)

dn3 :: Maybe (Integer, Integer)
dn3 = do
    a <- Just 1
    b <- Just 2
    return (a,b)

dn5 :: Maybe (Integer, Integer)
dn5 = do
    a <- Just 1
    b <- Nothing
    return (a,b)