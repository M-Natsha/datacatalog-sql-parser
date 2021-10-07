DELETE tableA
FROM tableA
INNER JOIN tableB u on (u.qlabel = tableA.entityrole AND u.fieldnum = tableA.fieldnum) 
WHERE (LENGTH(tableA.memotext) NOT IN (8,9,10)
OR tableA.memotext NOT LIKE '%/%/%')
AND (u.FldFormat = 'Date')