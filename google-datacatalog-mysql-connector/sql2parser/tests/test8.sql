INSERT INTO
  table1 (column1, column2)
SELECT
  *,
  col1 as x,
  func(col2, col3)
FROM
  table2;
INSERT INTO
  t1 (
    SELECT
      *
    FROM
      t2
      LEFT JOIN t3 ON t2.id = t3.id
  )