#!/usr/bin/env python

import pyproj
import psycopg2
import math
import time
import sys
import os
import json

import shapely.wkt
from shapely.ops import cascaded_union

from habitats import *

min_size            = 50000 # m^2
#max_size            = 1000000 # m^2
#max_size_water      = 1000000 # m^2
search_distance     = 100.0 # m
merge_distance      = {
   H_URBAN:         100.0,   # m
   H_WOOD:          10.0,   # m
   H_WETLAND:       10.0,   # m
   H_WATER:         10.0,   # m
   H_RIVER:         10.0,   # m
   H_GRASSLAND:     20.0,   # m
   H_ARABLE:        2.0,    # m
   H_SEMINATURAL:   2.0,    # m
   H_MOUNTAIN:      10.0    # m
}
merge_buffer        = {
   H_URBAN:         50.0,   # m
   H_WOOD:          10.0,   # m
   H_WETLAND:       30.0,   # m
   H_WATER:         30.0,   # m
   H_RIVER:         30.0,   # m
   H_GRASSLAND:     30.0,   # m
   H_ARABLE:        30.0,   # m
   H_SEMINATURAL:   30.0,   # m
   H_MOUNTAIN:      10.0    # m
}
output_max_simplify = 50.0 # m

n_workers = min(os.sysconf(os.sysconf_names['SC_NPROCESSORS_ONLN']), 64)
areas = {}
h_types = {}
merge = {}
unmerged = {}
ignore = {}

def add_merge(a, b):
    if a not in merge:
        merge[a] = { b: True }
    else:
        merge[a][b] = True
    if b not in merge:
        merge[b] = { a: True }
    else:
        merge[b][a] = True

def adaptive_simplify(geom):
    s = output_max_simplify
    while s > 1.0:
        new_geom = geom.simplify(s)
        if new_geom.is_valid:
            return new_geom
        s = s / 2.0
    return geom

def process_search(label, habitats):
    print label, 'start', time.time()
    
    conn = psycopg2.connect("dbname=gis")
    cur = conn.cursor()
    
    for (_id, _type, geom, area) in habitats:
        if worker_shutdown_required():
            print label, 'shutdown'
            return
        
        cur.execute("SELECT polygon_id, h_type, ST_Distance(ST_Transform(geom, 27700), ST_Transform(ST_SetSRID(ST_GeomFromText(%s),4326),27700)), ST_Area(ST_Transform(geom, 27700)) FROM habitat_raw WHERE (polygon_id != %s) AND (h_type = %s) AND ST_DWithin(ST_Transform(geom, 27700), ST_Transform(ST_SetSRID(ST_GeomFromText(%s),4326),27700), %s)", (geom, _id, _type, geom, search_distance))
        matches = cur.fetchall()

        if len(matches) == 0:
            print ' ', 'nothing found'

        for (o_id, o_type, o_dist, o_area) in matches:
            print ' ', o_id, o_dist
            if (o_id not in ignore) and (o_dist <= merge_distance[_type]):
                add_merge(o_id, _id)
    
    print label, 'finish', time.time()

    conn.commit()
    conn.close()

def process_merge(label, queue, merge_sets):
    print label, 'start', time.time()
    
    conn = psycopg2.connect("dbname=gis")
    cur = conn.cursor()
    
    for to_merge in merge_sets:
        geoms = []
        validity = None

        print label, 'load', to_merge
        cur.execute("SELECT polygon_id, ST_AsText(ST_Transform(geom, 27700)) FROM habitat_raw WHERE polygon_id = ANY(%s)", (to_merge,))
        _geoms = cur.fetchall()
        for (m_id, g) in _geoms:
            geom = shapely.wkt.loads(g)
            geoms.append(geom.buffer(merge_buffer[h_types[m_id]]))
        validity = map(lambda x:x.is_valid, geoms)

        _id = to_merge[0]
        did_merge = False
        print label, 'merging', _id, h_types[_id], len(to_merge)
        if (len(validity) > 1) and (False not in validity):
            new_geom = cascaded_union(geoms)
            if new_geom.geom_type == 'Polygon':
                new_geom = adaptive_simplify(new_geom)

                print ' ', new_geom.geom_type, len(str(new_geom))
                cur.execute("SELECT ST_Area(ST_SetSRID(ST_GeomFromText(%s), 27700))", (str(new_geom),))
                new_area = (cur.fetchall())[0][0]
                print ' ', 'area', new_area
                
                if new_area < min_size:
                    print ' ', 'too small'
                else:
                    print ' ', 'commit'
                    cur.execute("INSERT INTO habitat (polygon_id, h_type, geom) VALUES(%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromText(%s), 27700),4326))", (_id, h_types[_id], str(new_geom)))
                    conn.commit()
                    did_merge = True
            else:
                print ' ', 'union is not polygon'
        else:
            print ' ', 'seems to contain invalid parts; ignoring...'

        if not did_merge:
            print ' ', 'not merged'
            for __id in to_merge:
                queue.put(__id)
    
    print label, 'finish', time.time()

    conn.commit()
    conn.close()
    queue.close()

def cmp_areas(x, y):
    return int(areas[x]) - int(areas[y])

def cmp_sets(x, y):
    return len(x) - len(y)

def fold_candidates_flat(_id):
    ncs = []
    search = [ _id ]
    while len(search) > 0:
        __id = search.pop()
        if unmerged[__id]:
            ncs.append(__id)
            unmerged[__id] = False
            cs = merge[__id].keys()
            cs = sorted(cs, cmp=cmp_areas, reverse=True)
            for c_id in cs:
                if unmerged[c_id]:
                    search.append(c_id)
    return ncs

def fold_candidates(_id):
    cs = merge[_id].keys()
    cs = sorted(cs, cmp=cmp_areas, reverse=True)
    ncs = [_id]
    unmerged[_id] = False
    for c_id in cs:
        if unmerged[c_id]:
            ncs.extend(fold_candidates(c_id))
    return ncs

### main program

mode = 'corine'
if len(sys.argv) >= 2:
    mode = sys.argv[1]
if mode == 'lcm':
    for k in merge_distance.keys():
        merge_distance[k] /= 2.0
    merge_distance[H_ARABLE] = 0.0

sys.setrecursionlimit(10000)
conn = psycopg2.connect("dbname=gis")
cur = conn.cursor()

cur.execute("SELECT polygon_id, h_type, ST_AsText(geom), ST_Area(ST_Transform(geom, 27700)) FROM habitat_raw ORDER BY ST_Area(ST_Transform(geom, 27700)) ASC;")
results = cur.fetchall()

# First filter the none habitats as these are not used
filtered_results = []
for (_id, _type, geom, area) in results:
    print 'habitat type', type_map[_type], _type, 'id', _id, 'area', area
    ok = True
    ok = ok and (_type != H_NONE)
    #ok = ok and (area <= max_size)
    #ok = ok and (not ((_type == H_WATER) and (area >= max_size_water)))
    if ok:
        filtered_results.append((_id, _type, geom, area))
    else:
        print ' ', 'ignore', _id
        ignore[_id] = True
results = filtered_results

for (_id, _type, geom, area) in results:
    areas[_id] = area
    h_types[_id] = _type
    if _id not in unmerged:
        unmerged[_id] = True

# do parallel search
do_thread_par(results, n_workers, process_search)

# compute merge
ids = merge.keys()
ids = sorted(ids, cmp=cmp_areas, reverse=True)
merge_sets = []
for _id in ids:
    if unmerged[_id]:
        merge_sets.append(fold_candidates_flat(_id))

ids = unmerged.keys()
for _id in ids:
    if not unmerged[_id]:
        del unmerged[_id]

# sort the work by size
#  this will allow even distribution by do_process_par
merge_sets = sorted(merge_sets, cmp=cmp_sets, reverse=True)

# perform parallel merge
result = do_process_par(merge_sets, n_workers, process_merge)
for _id in result:
    unmerged[_id] = True

# handle remaining data
ids = unmerged.keys()
for _id in ids:
    if areas[_id] >= min_size:
        print 'copy', _id
        cur.execute("SELECT ST_AsText(ST_Transform(geom, 27700)) FROM habitat_raw WHERE polygon_id = %s", (_id,))
        geom = shapely.wkt.loads((cur.fetchall())[0][0])
        geom = geom.buffer(merge_buffer[h_types[_id]])
        geom = adaptive_simplify(geom)
        cur.execute("INSERT INTO habitat (polygon_id, h_type, geom) VALUES(%s,%s,ST_Transform(ST_SetSRID(ST_GeomFromText(%s),27700),4326))", (_id, h_types[_id], str(geom)))
    else:
        print 'skip', _id
conn.commit()

# shutdown
conn.close()
