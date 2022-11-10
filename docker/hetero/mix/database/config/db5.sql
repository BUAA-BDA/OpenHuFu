drop table if exists traffic;
create table traffic (
    id int,
    location geometry
);

insert into traffic values
(0, ST_GeomFromText('Point(115.0 40.0)', 4326)),
(1, ST_GeomFromText('Point(115.5 40.0)', 4326)),
(2, ST_GeomFromText('Point(116.0 40.0)', 4326)),
(3, ST_GeomFromText('Point(115.0 40.5)', 4326)),
(4, ST_GeomFromText('Point(115.5 40.5)', 4326)),
(5, ST_GeomFromText('Point(116.0 40.5)', 4326)),
(7, ST_GeomFromText('Point(115.0 41.0)', 4326)),
(8, ST_GeomFromText('Point(115.5 41.0)', 4326)),
(9, ST_GeomFromText('Point(116.0 41.0)', 4326));