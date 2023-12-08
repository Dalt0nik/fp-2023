module Lessons.Draft0 () where
    
factorial n = if n > 1
    then n * factorial (n - 1) else 1

factorial' = helper 1
helper acc n = if n > 1
    then helper (acc * n) (n - 1)
    else acc
--factorial'' n' = helper 1 n'
factorial'' :: Integer -> Integer
factorial'' = helper 1
    where helper acc n = if n > 1
            then helper (acc * n) (n - 1)
            else acc

--let smth
--in smth arg

k x y = x

add x y = x + y

(*+*) a b = a * a + b * b



--"Numa a =>" функцию add можем вызывать только для аргументов типа Num ("интерфейс")