-- create table routesGIST as table routes;
-- create index routes_route_gist_index ON routes USING Gist(route);

drop table if exists aggregated_vessel_positions_SPGIST;
create table aggregated_vessel_positions_SPGIST as table aggregated_vessel_positions;
drop index if exists routes_route_spgist_index;
create index routes_route_spgist_index ON aggregated_vessel_positions_SPGIST USING gist(route);

-- This doesn't work for some reason. It says there is no such operator, but in the documentation there is??
-- select * from aggregated_vessel_positions_ WHERE eintersects(geometry 'Polygon((0 0,0 1,1 1,1 0,0 0))', route);
select * from aggregated_vessel_positions_SPGIST WHERE eintersects(st_setsrid(ST_MakeEnvelope(-88.0, 41.80, -87.0, 41.87),4326), route);
select * from aggregated_vessel_positions WHERE eintersects(st_setsrid(ST_MakeEnvelope(-88.0, 41.80, -87.0, 41.87),4326), route);

select mmsi, (timestamps(route)) as times
from routes
where mmsi = 477150500;
-- ST_MakeEnvelope(-88.0, 41.80, -87.0, 41.87, 4326);
-- select srid(geom) from routes;
select * from aggregated_vessel_positions_SPGIST where mmsi = 366983440;

