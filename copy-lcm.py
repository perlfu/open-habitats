#!/usr/bin/env python

import re
import sys
import psycopg2
import os

import shapely.wkt
from shapely.geometry import Point
from shapely.ops import cascaded_union

from habitats import *
from config import *

n_workers = os.sysconf(os.sysconf_names['SC_NPROCESSORS_ONLN'])

def process_polygons(label, queue, polygons):
    conn = psycopg2.connect("dbname=gis")
    cur = conn.cursor()

    try:
        skipped = 0
        for (polygon_id, gid) in polygons:
            cur.execute("SELECT bh, bhsub, ST_AsText(ST_Transform(the_geom,27700)) FROM lcm2007_polygon WHERE gid = %s", (gid,))
            results = cur.fetchall()
            if len(results) > 0:
                (bh, bhsub, wkt) = results[0]
                polygon = shapely.wkt.loads(wkt)

                if polygon.is_valid and polygon.geom_type == 'Polygon':
                    polygon = polygon.simplify(initial_simplify)
                else:
                    print 'invalid polygon generated'
                    sys.exit(1)

                polygon_type = map_lcm(bh, bhsub) 
                    
                print label, polygon_id, len(str(polygon))
                cur.execute("INSERT INTO habitat_raw (polygon_id, h_type, geom) VALUES(%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromText(%s),27700),4326));",
                    (polygon_id, polygon_type, str(polygon)))
                conn.commit()
            else:
                skipped += 1
            if not queue.empty():
                return
        print label, "skipped =", skipped
    except KeyboardInterrupt:
        pass

    conn.commit()
    conn.close()
    queue.close()

def load_polygon_ids(min_x, max_x, min_y, max_y):
    conn = psycopg2.connect("dbname=gis")
    cur = conn.cursor()

    cur.execute("SELECT gid FROM lcm2007_polygon WHERE ST_Intersects(the_geom, ST_Transform(ST_SetSRID(ST_MakeBox2D(ST_Point(%s, %s), ST_Point(%s, %s)),27700),4326));", (min_x, min_y, max_x, max_y))
    data = cur.fetchall()
    for i in range(len(data)):
        data[i] = ((i + 1), data[i][0])

    conn.commit()
    conn.close()

    return data

### main program

data = load_polygon_ids(min_x, max_x, min_y, max_y)
do_process_par(data, n_workers, process_polygons) 
