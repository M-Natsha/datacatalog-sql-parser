SELECT
  *,
  A,
  A AS B,
  B.X as A,
  B.X,
  B.X.Y,
  A || 'hi'
FROM
  T1