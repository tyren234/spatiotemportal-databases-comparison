drop table vessel_positions;
drop table temp_vessel_positions;
-- Step 1: Define the Table Schema
CREATE TABLE vessel_positions
(
    id                SERIAL PRIMARY KEY,
    mmsi              INT,
    base_datetime     TIMESTAMPTZ,
    lat               REAL,
    lon               REAL,
    sog               REAL,
    cog               REAL,
    heading           REAL,
    vessel_name       TEXT,
    imo               TEXT,
    call_sign         TEXT,
    vessel_type       REAL,
    status            REAL,
    length            REAL,
    width             REAL,
    draft             REAL,
    cargo             REAL,
    transceiver_class TEXT,
    route             tgeompoint -- Temporal geometry point
);

-- Step 2: Create a Temporary Table for CSV Import
CREATE TABLE temp_vessel_positions
(
    mmsi              INT,
    base_datetime     TIMESTAMPTZ,
    lat               REAL,
    lon               REAL,
    sog               REAL,
    cog               REAL,
    heading           REAL,
    vessel_name       TEXT,
    imo               TEXT,
    call_sign         TEXT,
    vessel_type       REAL,
    status            REAL,
    length            REAL,
    width             REAL,
    draft             REAL,
    cargo             REAL,
    transceiver_class TEXT
);

-- Step 3: Import the CSV Data
-- psql -h 172.17.0.1 -p 55000 -U postgres -d aisdata -c "\copy temp_vessel_positions FROM '/home/tymon/Documents/gits/spatiotemportal-databases-comparison/data/AIS_2020_12_31.csv' DELIMITER ',' CSV HEADER;"

-- Step 3.1: Remove duplicates
SELECT mmsi, base_datetime, lat, lon, COUNT(*)
FROM temp_vessel_positions
GROUP BY mmsi, base_datetime, lat, lon
HAVING COUNT(*) > 1;


DELETE
FROM temp_vessel_positions a
    USING temp_vessel_positions b
WHERE a.ctid < b.ctid
  AND a.mmsi = b.mmsi
  AND a.base_datetime = b.base_datetime
  AND a.lat = b.lat
  AND a.lon = b.lon;

-- Step 4: Insert Data into the Main Table
INSERT INTO vessel_positions (mmsi, base_datetime, lat, lon, sog, cog, heading, vessel_name, imo, call_sign,
                              vessel_type, status, length, width, draft, cargo, transceiver_class, route)
SELECT mmsi,
       base_datetime,
       lat,
       lon,
       sog,
       cog,
       heading,
       vessel_name,
       imo,
       call_sign,
       vessel_type,
       status,
       length,
       width,
       draft,
       cargo,
       transceiver_class,
       tgeompoint( -- Create a temporal point
               st_setsrid(st_makepoint(lon, lat), 4326), -- Spatial point
               base_datetime -- Timestamp of that point
       )
FROM temp_vessel_positions;

-- Step 5: Handle Multiple Positions Per Vessel and Save to a New Table

-- 1. Create a New Table for Aggregated Data
DROP TABLE IF EXISTS aggregated_vessel_positions;
CREATE TABLE aggregated_vessel_positions
(
    mmsi              int,
    route             tgeompoint
);

-- 2. Co ciekawe to jest kilka MMSI, które z jakiegoś powodu mają kilka rekordów o 22:00. 9 mmsi,
-- każde z nich po dwa rekordy. Nie wiem co się wtedy działo, ale poniższe kwerendy sprawdzają
-- czy takie przypadki są i jeżeli są to je usuwają.

SELECT mmsi, base_datetime, COUNT(*)
FROM temp_vessel_positions
GROUP BY mmsi, base_datetime
HAVING COUNT(*) > 1;

DELETE FROM temp_vessel_positions
WHERE ctid IN (
    SELECT ctid
    FROM (
        SELECT
            ctid,
            ROW_NUMBER() OVER (PARTITION BY mmsi, base_datetime ORDER BY ctid) AS row_num
        FROM
            temp_vessel_positions
    ) AS subquery
    WHERE row_num > 1
);


-- 2.1 Aggregate Multiple Positions into a Sequence for Each Vessel and Insert into New Table:

WITH position_sequences AS (SELECT mmsi,
                                   tgeompointseq(array_agg(
                                           tgeompoint(
                                                   st_setsrid(st_makepoint(lon, lat), 4326),
                                                   base_datetime
                                           )
                                           ORDER BY base_datetime
                                                 )) AS route
                            FROM temp_vessel_positions
                            GROUP BY mmsi
)
INSERT INTO aggregated_vessel_positions (mmsi, route)
SELECT mmsi,
       route
FROM position_sequences;


-- Step 6: Remove not needed tables and check the output out!

drop table temp_vessel_positions;
select *, route::geometry from aggregated_vessel_positions;

