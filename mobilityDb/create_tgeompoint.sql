-- Tworzy tgeompoint dla jednego mmsi
with unique_aisdata as (
    select distinct a.mmsi, a.basedatetime, a.geom
    from aisdata as a
    where a.mmsi = 366982180
)
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