#!/usr/bin/env python

import sys
import psycopg2
import json

import shapely.wkt

from habitats import *

limits = {
    H_NONE:         0,
    H_URBAN:        100,
    H_WOOD:         0,
    H_WETLAND:      30,
    H_WATER:        20,
    H_RIVER:        20,
    H_GRASSLAND:    30,
    H_ARABLE:       150,
    H_SEMINATURAL:  30,
    H_MOUNTAIN:     0
}

if len(sys.argv) < 2:
    print 'output-habitats <destination>'
    sys.exit(1)

destination = sys.argv[1]

conn = psycopg2.connect("dbname=gis")
cur = conn.cursor()

cur.execute("SELECT link_id, polygon_id FROM habitat_link")
results = cur.fetchall()
transports = []
_links = {}
links = {}
for (link_id, polygon_id) in results:
    if polygon_id not in links:
        links[polygon_id] = set()
    links[polygon_id].add(link_id)
    _links[link_id] = True
for link_id in _links.keys():
    transports.append({
        'id': str(link_id),
        'type': 'land'
    })

results = []
for h_type in limits.keys():
    if limits[h_type] > 0:
        cur.execute("SELECT polygon_id, h_type, ST_AsText(geom) FROM habitat WHERE h_type = %s ORDER BY ST_Area(ST_Transform(geom, 27700)) DESC LIMIT %s;", (h_type, limits[h_type]))
        results.extend(cur.fetchall())

habitats = []
for (_id, _type, geom) in results:
    print _id, _type
    shape = shapely.wkt.loads(geom)
    points = []
    for (x, y) in shape.exterior.coords:
        points.append(float("%.6f" % x))
        points.append(float("%.6f" % y))

    if _id in links:
        _links = map(str, links[_id])
    else:
        _links = []

    habitats.append({
        'id': str(_id),
        'type': type_map[_type],
        'points': points[0:-2],
        'regulation': [],
        'links': _links
    })

conn.close()

fh = open(destination, 'wb')
fh.write("ESD.modelDescriptor['transports'] = ")
json.dump(transports, fh, sort_keys=True, indent=2)
fh.write(";\n")
fh.write("ESD.modelDescriptor['habitats'] = ")
json.dump(habitats, fh, sort_keys=True, indent=2)
fh.write(";\n")
fh.close()
