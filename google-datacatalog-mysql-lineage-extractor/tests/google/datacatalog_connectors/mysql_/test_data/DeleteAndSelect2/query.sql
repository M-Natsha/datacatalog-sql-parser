DELETE FROM tableA WHERE entitynum IN (
    SELECT tableA.entitynum FROM tableA q
      INNER JOIN tableB u on (u.qlabel = q.entityrole AND u.fieldnum = q.fieldnum) 
    WHERE (LENGTH(q.memotext) NOT IN (8,9,10) OR q.memotext NOT LIKE '%/%/%')
      AND (u.FldFormat = 'Date')
)