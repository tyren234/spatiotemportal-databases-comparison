select sum(memsize(route)) / 1000000
from aggregated_vessel_positions;
select tempsubtype(route)
from aggregated_vessel_positions;
select interp(route)
from aggregated_vessel_positions;
select getTime(route)
from aggregated_vessel_positions;
select timespan(route)
from aggregated_vessel_positions;
select valueset(route)
from aggregated_vessel_positions;
select astext(valueset(route))
from aggregated_vessel_positions;
select endvalue(route)
from aggregated_vessel_positions;
select startvalue(route)
from aggregated_vessel_positions;
select valueattimestamp(route, '2020-12-31 12:00:00+00')
from aggregated_vessel_positions;
select duration(route)
from aggregated_vessel_positions;
select lowerinc(route)
from aggregated_vessel_positions;
select upperinc(route)
from aggregated_vessel_positions;
select instantn(route, 5)
from aggregated_vessel_positions; --nty element z sequence
select starttimestamp(route)
from aggregated_vessel_positions;
select endtimestamp(route)
from aggregated_vessel_positions;
select timestampn(route, 10)
from aggregated_vessel_positions;
select timestamps(route)
from aggregated_vessel_positions;

-- Time query
select astext(attime(route,
                     tstzspan('[2020-12-31T00:00:00Z, 2020-12-31T00:59:00Z]')
              ))
from aggregated_vessel_positions;

-- Spatial query
select *
from aggregated_vessel_positions_SPGIST
where eintersects(st_setsrid(ST_MakeEnvelope(-88.0, 41.80, -87.0, 41.87), 4326), route);


select *
from (select atgeometrytime(
                     route,
                     st_setsrid(ST_MakeEnvelope(-88.0, 41.80, -87.0, 41.87), 4326),
                     tstzspan('[2020-12-31T00:00:00Z, 2020-12-31T23:59:00Z]')
             ) as vessels
      from aggregated_vessel_positions) as a
where a.vessels <> null;

