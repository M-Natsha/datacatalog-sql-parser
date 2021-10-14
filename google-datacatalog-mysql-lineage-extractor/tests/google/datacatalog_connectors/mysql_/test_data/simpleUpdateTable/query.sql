UPDATE
  cars C,
  users U,
  (SELECT * FROM T1 where x > 1) as t
SET
  vendor = U.first_name,
  x = y
WHERE
  C.id = U.id;