select distinct mmsi
from aisdata;


with unique_aisdata as (select distinct a.mmsi, a.basedatetime, a.geom
                        from aisdata as a
                        where a.mmsi = 366982180)
select m.mmsi,
       (select (
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
                   )::tgeompoint) as route
from (select distinct mmsi from aisdata) as m;

-- Utworzenie tabeli
drop table routes;
CREATE TABLE routes
(
    mmsi  int,
    route tgeompoint
);

-- Wstawienie danych tgeompoint do tabeli

-- WITH unique_aisdata AS (
--     SELECT DISTINCT a.mmsi, a.basedatetime, a.geom
--     FROM aisdata AS a
-- )
INSERT INTO routes (mmsi, route)
SELECT
    m.mmsi,
    (concat(
        '[',
        array_to_string(
            array(
                SELECT concat(st_astext(b.geom), '@', b.basedatetime)
                FROM aisdata AS b
                WHERE b.mmsi = m.mmsi
                ORDER BY b.basedatetime -- Sortowanie danych wewnątrz podzapytania
            ),
            ','
        ),
        ']'
    )::tgeompoint) AS route
FROM (SELECT DISTINCT mmsi FROM aisdata) AS m;

-- Podejście z pętlą
DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN (SELECT DISTINCT mmsi FROM aisdata) LOOP
        INSERT INTO routes (mmsi, route)
        SELECT
            rec.mmsi,
            (concat(
                '[',
                array_to_string(
                    array(
                        SELECT concat(st_astext(b.geom), '@', b.basedatetime)
                        FROM aisdata AS b
                        WHERE b.mmsi = rec.mmsi
                        ORDER BY b.basedatetime
                    ),
                    ','
                ),
                ']'
            )::tgeompoint) AS route;
    END LOOP;
END $$;

-- Znajdowanie duplikatów
SELECT mmsi, basedatetime, count(*)
FROM aisdata
GROUP BY mmsi, basedatetime
HAVING count(*) > 1;

-- Usuwanie duplikatów
DELETE FROM aisdata
WHERE (mmsi, basedatetime) IN (
    SELECT mmsi, basedatetime
    FROM aisdata
    GROUP BY mmsi, basedatetime
    HAVING count(*) > 1
);


-- Dodanie nowej kolumny
ALTER TABLE routes
ADD COLUMN geom geometry;

-- Aktualizacja tabeli z wartościami geometrycznymi
UPDATE routes
SET geom = route::geometry;

select st_astext(geom) from routes;