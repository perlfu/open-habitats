#!/usr/bin/env python

import subprocess
import pyproj
import psycopg2
import math
import time
import threading
import sys
import os

from habitats import *
from config import *

# globals
shutdown = False
etrs89 = pyproj.Proj(init='epsg:3035')
osgb36 = pyproj.Proj(init='epsg:27700')
wgs84 = pyproj.Proj(init='epsg:4326')
output_type_map = None
output_stats = None
ref_index = None

def dict_with_keys(keys, value=0):
    d = {}
    for k in keys:
        d[k] = value
    return d

def dict_with_keys2(keys, value=0):
    d = {}
    for k in keys:
        d[k] = dict_with_keys(keys, value=value)
    return d

def grid_points(x, y, width, height, step):
    for xi in range(int(width / step)):
        for yi in range(int(height / step)):
            yield ((x + (xi * step)), (y + (yi * step)))

def generate_points(x, y, width, height, step, mapper, queue):
    grid = []
    for xi in range(int(width / step)):
        for yi in range(int(height / step)):
            this_x = (x + (xi * step))
            this_y = (y + (yi * step))
            #print 'put', (this_x, this_y)
            queue.put(mapper((this_x, this_y)))

def osgb36_to_wgs84(coord):
    if not isinstance(coord, list):
        (easting, northing) = coord
        lon, lat = pyproj.transform(osgb36, wgs84, easting, northing)
        return (lon, lat)
    else:
        r = []
        for (easting, northing) in coord:
            lon, lat = pyproj.transform(osgb36, wgs84, easting, northing)
            r.append((lon, lat))
        return r

def osgb36_to_etrs89(coord):
    if not isinstance(coord, list):
        (easting, northing) = coord
        e, n = pyproj.transform(osgb36, etrs89, easting, northing)
        return (e, n)
    else:
        r = []
        for (easting, northing) in coord:
            e, n = pyproj.transform(osgb36, etrs89, easting, northing)
            r.append((e, n))
        return r

def open_db():
    conn = psycopg2.connect("dbname=gis")
    cur = conn.cursor()
    return (conn, cur)

def process_corine_points(label, coords):
    global shutdown

    commit_buffer = 1000
    (conn, cur) = open_db()

    found = 0
    missing = 0
    extra = 0
    skipped = 0
    pending = 0
    n = 0

    print label, 'start', time.time()

    for (ref, easting, northing) in coords:
        if shutdown:
            print label, 'shutdown'
            return

        n += 1
        cur.execute("SELECT ref, code FROM cell WHERE ref = %s", (ref,))
        results = cur.fetchall()
        code = None
        _ref = None
        if len(results) != 0:
            (_ref, code) = results[0]
        
        if code is None:
            cur.execute("SELECT code, name FROM clc_tiled WHERE ST_Within(ST_SetSRID(ST_Point(%s,%s),3035),geom)", (easting, northing))
            results = cur.fetchall()
            if len(results) == 1:
                (code, name) = results[0]
                if not _ref:
                    cur.execute("INSERT INTO cell (ref, code) VALUES(%s, %s)", (ref, code))
                else:
                    cur.execute("UPDATE cell SET code = %s WHERE ref = %s", (code, ref))
                pending += 1
                found += 1
            elif len(results) > 1:
                print label, "surplus", (ref, easting, northing)
                extra += 1
            else:
                print label, "missing", (ref, easting, northing)
                missing += 1
        
            if pending >= commit_buffer:
                print label, "commit", n
                conn.commit()
                pending = 0
        else:
            skipped += 1

    conn.commit()

    print label, 'finish', time.time()
    print label, "found = %d, missing = %d, extra = %d, skipped = %d" % (found, missing, extra, skipped)

    cur.close()
    conn.close()

def process_lcm_points(label, coords):
    global shutdown

    commit_buffer = 1000
    (conn, cur) = open_db()

    found = 0
    missing = 0
    extra = 0
    skipped = 0
    pending = 0
    n = 0

    print label, 'start', time.time()

    for (ref, easting, northing) in coords:
        if shutdown:
            print label, 'shutdown'
            return

        n += 1
        cur.execute("SELECT ref, code FROM cell WHERE ref = %s", (ref,))
        results = cur.fetchall()
        code = None
        _ref = None
        if len(results) != 0:
            (_ref, code) = results[0]
        
        if code is None:
            cur.execute("SELECT bh, bhsub FROM lcm2007_polygon WHERE ST_Within(ST_SetSRID(ST_Point(%s,%s),4326),the_geom)", (easting, northing))
            results = cur.fetchall()
            if len(results) == 1:
                (bh, bhsub) = results[0]
                code = "%03d" % (map_lcm(bh, bhsub),)
                if not _ref:
                    cur.execute("INSERT INTO cell (ref, code) VALUES(%s, %s)", (ref, code))
                else:
                    cur.execute("UPDATE cell SET code = %s WHERE ref = %s", (code, ref))
                pending += 1
                found += 1
            elif len(results) > 1:
                print label, "surplus", (ref, easting, northing)
                extra += 1
            else:
                print label, "missing", (ref, easting, northing)
                missing += 1
        
            if pending >= commit_buffer:
                print label, "commit", n
                conn.commit()
                pending = 0
        else:
            skipped += 1

    conn.commit()

    print label, 'finish', time.time()
    print label, "found = %d, missing = %d, extra = %d, skipped = %d" % (found, missing, extra, skipped)

    cur.close()
    conn.close()

def pick_category(data):
    # try first element
    if len(data) == 1:
        return data[0]
    
    # try reduction
    r = {}
    for v in data:
        r[v] = None
    data = r.keys()
    
    # try first element
    if len(data) == 1:
        return data[0]

    # try fixed priority
    for x in ['water', 'trans', 'build', 'none', 'wood', 'grass', 'heath', 'scrub', 'marsh']:
        if x in data:
            return x
    
    # else...
    print 'undecided', data
    return None

def process_osm_points(label, coords):
    global shutdown

    commit_buffer = 1000
    (conn, cur) = open_db()

    found = 0
    missing = 0
    extra = 0
    skipped = 0
    pending = 0
    n = 0

    print label, 'start', time.time()

    for (ref, easting, northing) in coords:
        if shutdown:
            print label, 'shutdown'
            return

        n += 1
        
        cur.execute("SELECT ref, category FROM cell WHERE ref = %s", (ref,))
        results = cur.fetchall()
        category = None
        _ref = None
        if len(results) != 0:
            (_ref, category) = results[0]
        
        if category is None:
            cs = []
            for table in ['planet_osm_point', 'planet_osm_line', 'planet_osm_polygon']:
                cur.execute("SELECT o.name,o.aeroway,o.building,o.highway,o.junction,o.landuse,o.man_made,o.natural,o.railway,o.surface,o.water,o.waterway,o.wetland,o.wood,ST_Distance(way,ST_Transform(ST_SetSRID(ST_Point(%s,%s),4326),900913)) AS distance FROM " + table + " AS o WHERE (boundary is null) AND (way is not null) AND ST_DWithin(way,ST_Transform(ST_SetSRID(ST_Point(%s,%s),4326),900913),%s) ORDER BY distance", (easting, northing, easting, northing, 15.0))
                results = cur.fetchall()
               
                for result in results:
                    (name, aeroway, building, highway, junction, landuse, man_made, natural, railway, surface, water, waterway, wetland, wood, distance) = result
                    c = { 'table': table, 'name': name, 'aeroway': aeroway, 'building': building, 'highway': highway, 'junction': junction, 'landuse': landuse, 'man_made': man_made, 'natural': natural, 'railway': railway, 'surface': surface, 'water': water, 'waterway': waterway, 'wetland': wetland, 'wood': wood, 'distance': distance }
                    ks = c.keys()
                    for k in ks:
                        if c[k] is None:
                            del c[k]
                    cs.append(c)
            
            category = None

            if len(cs) > 0:
                found += 1
                best = None
                best_d = {}
                for c in cs:
                    category = classify_osm(c)
                    if category:
                        if not best:
                            best = [category]
                        else:
                            best.append(category)
                        if category in best_d:
                            if c['distance'] < best_d[category]:
                                best_d[category] = c['distance']
                        else:
                            best_d[category] = c['distance']
                if best:
                    category = pick_category(sorted(best_d, key=best_d.get))

            if category:
                if not _ref:
                    cur.execute("INSERT INTO cell (ref, category) VALUES(%s, %s)", (ref, category))
                else:
                    cur.execute("UPDATE cell SET category = %s WHERE ref = %s", (category, ref))
                pending += 1
            else:
                missing += 1
            
            if pending >= commit_buffer:
                print label, "commit", n
                conn.commit()
                pending = 0
        else:
            skipped += 1

    conn.commit()

    print label, 'finish', time.time()
    print label, "found = %d, missing = %d, extra = %d, skipped = %d" % (found, missing, extra, skipped)

    cur.close()
    conn.close()

def compute_types(label, coords):
    global shutdown
    global output_type_map

    (conn, cur) = open_db()

    n = 0

    print label, 'start', time.time()

    curr = []
    refs = [curr]
    for (ref, easting, northing) in coords:
        curr.append(int(ref))
        if len(curr) >= 10000:
            curr = []
            refs.append(curr)

    for rs in refs:
        if shutdown:
            print label, 'shutdown'
            return
        
        mapping = {}
        cur.execute("SELECT ref,code,category FROM cell WHERE ref = ANY(%s);", (rs,))
        for (_ref, code, category) in cur.fetchall():
            mapping[_ref] = map_code_category(code, category)
            print "%12d '%s' '%s' => %d" % (_ref, code, category, mapping[_ref])

        for ref in rs:
            r = str(ref)
            x = int(r[0:6])
            y = int(r[6:12])
            idx = reference_index(x, y)

            if ref in mapping:
                output_type_map[idx] = mapping[ref]
            else:
                output_type_map[idx] = H_NONE # unknown
            n += 1

    print label, 'finish', time.time()
    
    cur.close()
    conn.close()

def verify_points(label, coords):
    global shutdown
    global output_stats
    (conn, cur) = open_db()

    n = 0
    checked = 0
    matched = 0
    matched_abs = 0
    errors = dict_with_keys2(habitat_types)
    coverage_h = dict_with_keys(habitat_types)
    coverage_l = dict_with_keys(habitat_types)

    print label, 'start', time.time()

    for (ref, easting, northing) in coords:
        if shutdown:
            print label, 'shutdown'
            return
        
        cur.execute("SELECT bh,bhsub FROM lcm2007_polygon WHERE ST_Within(ST_Transform(ST_SetSRID(ST_Point(%s,%s),27700),4326),the_geom)", (easting, northing))
        results = cur.fetchall()
        if len(results) >= 1:
            (bh, bhsub) = results[0]
            lcm_type = map_lcm(bh, bhsub)
        else:
            (bh, bhsub) = ('', '')
            lcm_type = H_NONE

        cur.execute("SELECT h_type FROM habitat WHERE ST_Within(ST_Transform(ST_SetSRID(ST_Point(%s,%s),27700),4326),geom)", (easting, northing))
        results = cur.fetchall()
        if len(results) >= 1:
            h_type = results[0][0]
        else:
            h_type = H_NONE

        n += 1
        coverage_h[h_type] += 1
        coverage_l[lcm_type] += 1
        if (h_type != H_NONE):
            checked += 1
            if (h_type == lcm_type):
                matched += 1
        if (h_type == lcm_type):
            matched_abs += 1
        else:
            # print ref, easting, northing, bh, bhsub, lcm_type, h_type
            errors[h_type][lcm_type] += 1

    print label, 'finish', time.time()
    print label, n, matched_abs, checked, matched

    output_stats['checked'] += checked
    output_stats['matched'] += matched
    output_stats['checked_abs'] += n
    output_stats['matched_abs'] += matched_abs
    for k in habitat_types:
        for t in habitat_types:
            output_stats['mapping_errors'][k][t] += errors[k][t]
        output_stats['coverage_h'][k] += coverage_h[k]
        output_stats['coverage_l'][k] += coverage_l[k]

    cur.close()
    conn.close()

# bootstrap
width = max_x - min_x
height = max_y - min_y
n_workers = min(os.sysconf(os.sysconf_names['SC_NPROCESSORS_ONLN']), 32)

base_x = int(math.floor(min_x / step))
base_y = int(math.floor(min_y / step))
base_width = int(math.ceil(width / step))

def reference_index(x, y):
    return int(((math.floor(y / step) - base_y) * base_width) + (math.floor(x / step) - base_x))

def gridref(coord):
    (x, y) = coord
    ref = '%06d%06d' % (math.floor(x / step) * int(step), math.floor(y / step) * int(step))
    return ref
    
# command line
mode = None
output = None
if len(sys.argv) >= 2:
    mode = sys.argv[1]
if len(sys.argv) >= 3:
    output = sys.argv[2]

if not ((mode == 'corine') or (mode == 'lcm') or (mode == 'osm') or (mode == 'route') or (mode == 'verify')):
    print 'compute-grid [corine|lcm|osm|route|verify]'
    sys.exit(1)

if mode == 'corine':
    map_point = osgb36_to_etrs89
    process_points = process_corine_points
elif mode == 'lcm':
    map_point = osgb36_to_wgs84
    process_points = process_lcm_points
elif mode == 'osm':
    map_point = osgb36_to_wgs84
    process_points = process_osm_points
elif mode == 'route':
    map_point = lambda x:x
    process_points = compute_types
    ref_index = reference_index
    output_type_map = bytearray(reference_index(max_x + step, max_y + step))
    print output_type_map
elif mode == 'verify':
    step = 100.0
    map_point = lambda x:x
    process_points = verify_points
    output_stats = { 
        'checked': 0,
        'checked_abs': 0,
        'matched': 0,
        'matched_abs': 0,
        'coverage_h': dict_with_keys(habitat_types),
        'coverage_l': dict_with_keys(habitat_types),
        'mapping_errors': dict_with_keys2(habitat_types)
    }

# compute work
points = 0
def generate_work():
    global points
    for p in grid_points(min_x, min_y, width, height, step):
        (easting, northing) = map_point(p)
        ref = gridref(p)
        points += 1
        yield (ref, easting, northing)

# process work
do_thread_par(generate_work(), n_workers, process_points) 

# output results
if output:
    # Print unique codes
    list_of_seen = []
    for i in range(len(output_type_map)):
        if (output_type_map[i] in list_of_seen) == False:
            print output_type_map[i]
            list_of_seen.append(output_type_map[i])

    fh = open(output, 'wb')
    fh.write("%06d %06d %06d %06d %d\n" % (step, base_x, base_y, base_width, len(output_type_map)))
    fh.write(output_type_map)
    fh.close()
if output_stats:
    print 'checked:', output_stats['checked']
    print 'matched:', output_stats['matched']
    print 'checked_abs:', output_stats['checked_abs']
    print 'matched_abs:', output_stats['matched_abs']
    errors = output_stats['checked_abs'] - output_stats['matched_abs']
    for k in habitat_types:
        v_h = output_stats['coverage_h'][k]
        v_l = output_stats['coverage_l'][k]
        pc_h = (float(v_h) / float(points)) * 100.0
        pc_l = (float(v_l) / float(points)) * 100.0
        print 'coverage %02d h:% 8d (% 2.1f), l:% 8d (% 2.1f)' % (k, v_h, pc_h, v_l, pc_l)
    for k in habitat_types:
        for t in habitat_types:
            v = output_stats['mapping_errors'][k][t]
            pc = (float(v) / float(errors)) * 100.0
            if v > 0:
                print "error %02d -> %02d, count: % 7d, % 2.1f" % (k, t, v, pc)
