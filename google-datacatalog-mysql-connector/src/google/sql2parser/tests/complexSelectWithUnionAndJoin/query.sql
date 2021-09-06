select
  *
from
  (
    select
      *
    from
      (
        select
          objectid
        from
          osm
        where
          tag = 'b'
        limit
          10
      )
    union
    select
      *
    from
      (
        select
          objectid
        from
          osm
          LEFT JOIN t2 ON osm.id = t2.id
        where
          tag = 'a'
        limit
          10
      )
  );