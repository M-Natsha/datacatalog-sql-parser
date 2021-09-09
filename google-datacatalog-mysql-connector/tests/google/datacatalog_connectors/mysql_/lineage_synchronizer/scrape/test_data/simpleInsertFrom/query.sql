INSERT INTO
  t1
SELECT
  t3.*,
  t4.id,
  t2.name,
  t2.desc
FROM
  t2
  LEFT JOIN t3 ON t2.id = t3.id
  LEFT JOIN t4 ON t3.id = t4.id
UNION ALL
SELECT
  *
FROM
  t3