UPDATE
  cars C
SET
  vendor = U.first_name
WHERE
  C.id = U.id;