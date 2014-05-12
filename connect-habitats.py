#!/usr/bin/env python

import sys
import psycopg2
import json

import shapely.wkt
from shapely.ops import cascaded_union

from habitats import *

type_map = {
   H_NONE: 'none',
   H_URBAN: 'hab_builtup',
   H_WOOD: 'hab_wood',
   H_WETLAND: 'hab_fwater',
   H_WATER: 'hab_fwater',
   H_RIVER: 'hab_fwater',
   H_GRASSLAND: 'hab_impgrass',
   H_ARABLE: 'hab_arable',
   H_SEMINATURAL: 'hab_sngrass',
   H_MOUNTAIN: 'hab_mountain'
}

conn = psycopg2.connect("dbname=gis")
cur = conn.cursor()

cur.execute("SELECT polygon_id, h_type, ST_AsText(geom) FROM habitat")
results = cur.fetchall()

h_types = {}
geoms = {}

for (_id, h_type, geom) in results:
    h_types[_id] = h_type
    geoms[_id] = geom

links = {}

for _id in h_types.keys():
    cur.execute("SELECT polygon_id FROM habitat WHERE (polygon_id != %s) AND ST_DWithin(ST_Transform(geom,27700), ST_Transform(ST_SetSRID(ST_GeomFromText(%s),4326),27700), 10)", (_id, geoms[_id]))
    matches = cur.fetchall()
    
    ms = []
    for (o_id,) in matches:
        ms.append(o_id)
    links[_id] = set(ms)
    links[_id].add(_id)

conns = {}

for link_id in h_types.keys():
    conns = links[link_id]
    if len(conns) > 1:
        print 'insert', link_id, conns 
        for value in conns:
            cur.execute("INSERT INTO habitat_link (link_id, polygon_id) VALUES(%s, %s)", (link_id, value))
    else:
        print 'skip', link_id, conns

#for _id in h_types.keys():
#    for link in links[_id]:
#        transitive = (set()).union(links[link])
#        rep = transitive.intersection(links[_id])
#        conns[str(rep)] = rep

#for (link_id, key) in enumerate(conns.keys()):
#    if len(conns[key]) > 1:
#        print 'insert', link_id, key
#        for value in conns[key]:
#            cur.execute("INSERT INTO habitat_link (link_id, polygon_id) VALUES(%s, %s)", (link_id, value))
#    else:
#        print 'skip', link_id, key

conn.commit()
conn.close()
