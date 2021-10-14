UPDATE
  cars C,
  users U
SET
  vendor = U.first_name
WHERE
  C.id = U.id;