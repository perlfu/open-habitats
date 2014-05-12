#!/usr/bin/env python

import re
import sys
import psycopg2
import os

from shapely.geometry import Point
from shapely.ops import cascaded_union

from habitats import *
from config import *

max_points = 500000
point_buffer = 10.0 # m
initial_simplify = 10.0 # m
n_workers = os.sysconf(os.sysconf_names['SC_NPROCESSORS_ONLN'])

def build_polygon(n_points, points):
    geom = []
    for p_str in points.split(' '):
        if len(p_str) > 1:
            (x, y) = p_str.lstrip().split(',')
            p = Point(int(x), int(y))
            geom.append(p.buffer(point_buffer))
    assert(len(geom) == n_points)

    polygon = cascaded_union(geom)
    if polygon.is_valid and polygon.geom_type == 'Polygon':
        return polygon.simplify(initial_simplify)
    else:
        print 'invalid polygon generated'
        sys.exit(1)

def process_polygons(label, queue, polygons):
    conn = psycopg2.connect("dbname=gis")
    cur = conn.cursor()

    try:
        skipped = 0
        for (polygon_id, polygon_type, n_points, points) in polygons:
            if n_points <= max_points:
                print label, polygon_id, polygon_type, n_points
                polygon = build_polygon(n_points, points)
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

if len(sys.argv) < 2:
    print "build-polygons <input-file>"
    sys.exit(1)

source = sys.argv[1]

polygon_re = re.compile(r'^polygon (\d+) \(type = (\d+)\) \(points = (\d+)\):\s*(.*?)\s*$')

data = []
fh = open(source, 'r')
for line in fh:
    m = polygon_re.match(line)
    if m:
        polygon_id = int(m.group(1))
        polygon_type = int(m.group(2))
        n_points = int(m.group(3))
        points = m.group(4)
        data.append((polygon_id, polygon_type, n_points, points))
        print 'loaded', polygon_id, n_points
fh.close()

do_process_par(data, n_workers, process_polygons)
