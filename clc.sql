create table clc06 (id int, code char(3), name varchar(10));
select AddGeometryColumn('clc06', 'geom', 3035, 'MULTIPOLYGON', 2);
create index clc06_gix on clc06 using GIST (geom);
create unique index on clc06 (id);
create index on clc06 (code);

create table clc_tiled (id int, code char(3), name varchar(10));
select AddGeometryColumn('', 'clc_tiled', 'geom', 3035, 'MULTIPOLYGON', 2);
create index clc_tiled_gix on clc_tiled using GIST (geom);

create table clc_tiling (x float, y float);
create index on clc_tiling (x, y);
