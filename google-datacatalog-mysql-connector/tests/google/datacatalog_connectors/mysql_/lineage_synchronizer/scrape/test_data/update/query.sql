UPDATE
  t1,
  t5,
  (
    SELECT
      *
    FROM
      t2
  ) as tx,
  (
    SELECT
      *
    FROM
      t2
      LEFT JOIN t3 ON x = y
  ) as t3
SET
  t1 = 5
