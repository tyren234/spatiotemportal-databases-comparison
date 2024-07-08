-- Tworzy tgeompoint dla jednego mmsi
with unique_aisdata as (select distinct a.mmsi, a.basedatetime, a.geom
                        from aisdata as a
                        where a.mmsi = 366982180)
select (
           concat(
                   '[',
                   array_to_string(
                           array(
                                   select concat(st_astext(b.geom), '@', b.basedatetime)
                                   from unique_aisdata as b
                                   order by b.basedatetime
                           ),
                           ','
                   ),
                   ']'
           )
           )::tgeompoint;

-- Znajdowanie duplikatów
SELECT mmsi, basedatetime, count(*)
FROM aisdata
GROUP BY mmsi, basedatetime
HAVING count(*) > 1;

select tgeompoint(geom, tstzset(set(array(select basedatetime
                                          from aisdata
                                          where mmsi = 366982180))))
from aisdata
where mmsi = 366982180
limit 1;

select mmsi, count(*) from aisdata group by mmsi;

-- TODO: zobaczyć czy to działa
-- 100 rekdów zrobiło się w 42 sekundy, ale pod prądem.
-- Pod prądem 10 rekordów robi się 4 sekundy, a 1 rekord 1s
-- 500 rekordów robi się 3m 26s czyli 206s. Zgodnie z przewidywaniami
-- wszystkie 13k robi się 1h 25m 33s
select a.mmsi,
       tgeompointseq(array(select tgeompoint(geom, basedatetime)
                                     from aisdata as b
                                     where b.mmsi = a.mmsi
                                     order by b.basedatetime))::geometry
from (select distinct mmsi from aisdata where mmsi = 338064000 ) as a;