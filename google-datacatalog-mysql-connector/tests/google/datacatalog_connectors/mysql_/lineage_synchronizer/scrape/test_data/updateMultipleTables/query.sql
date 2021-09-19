UPDATE  cars C,
 users U
SET
 vendor = first_name,
 U.last_name = "DUCK"
WHERE
 C.id = U.id;
