SELECT
  t1 as y,
  t2 as x
FROM
  t1
  LEFT JOIN t2 on t1.id = t2.id
UNION
  t5
  LEFT JOIN (
    t3
    UNION
      t4
  ) as X ON X.id = t5.id