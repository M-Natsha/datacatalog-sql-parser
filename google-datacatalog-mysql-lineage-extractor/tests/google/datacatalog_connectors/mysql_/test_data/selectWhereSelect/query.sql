SELECT
  *
FROM
  T1
WHERE T1.id > 
 (SELECT COUNT(*) FROM T2)