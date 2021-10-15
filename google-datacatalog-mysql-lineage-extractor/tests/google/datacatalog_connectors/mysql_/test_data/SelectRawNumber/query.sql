select
  *
from
  dept
where
  exists (
    select
      1
    from
      emp
    where
      emp.deptno = dept.deptno
  )