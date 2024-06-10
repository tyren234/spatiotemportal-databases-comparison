-- lat1 = 18.45949
-- lon1 = -89.92433
--
-- lat2 = 27.37022
-- lon2 = -66.08799

select mmsi, trajectory(route)
from (select mmsi, route from routes) as r
where tintersects(ST_MakeEnvelope(-95, 18, -66, 32), route, true) is not null;


select st_astext(trajectory((select route from routes where mmsi = 316004940)));
select trajectory((select route from routes where mmsi = 316004940));
select (select route from routes where mmsi = 316004940)::geometry;


select stbox('STBOX X((-95, 18), (-66, 32))')::geometry;

