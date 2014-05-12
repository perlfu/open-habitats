create table cell (ref bigint, code char(3), category char(10));
create unique index on cell (ref);

create table habitat_raw (polygon_id int, h_type int);
select AddGeometryColumn ('','habitat_raw','geom',4326,'POLYGON',2);
create index habitat_raw_gix on habitat_raw using GIST (geom);

create table habitat (polygon_id int, h_type int);
select AddGeometryColumn ('','habitat','geom',4326,'POLYGON',2);
create index habitat_gix on habitat using GIST (geom);

create table habitat_link (link_id int, polygon_id int);
create index on habitat_link (polygon_id);
