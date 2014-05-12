#!/usr/bin/env python

import subprocess
import pyproj
import psycopg2
import time
import threading
import os

def grid_points(x, y, width, height, step):
    grid = []
    for xi in range(int(width / step)):
        for yi in range(int(height / step)):
            grid.append(((x + (xi * step)), (y + (yi * step))))
    return grid

def osgb36_to_wgs84(coord):
    osgb36 = pyproj.Proj(init='epsg:27700')
    wgs84 = pyproj.Proj(init='epsg:4326')
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
    osgb36 = pyproj.Proj(init='epsg:27700')
    etrs89 = pyproj.Proj(init='epsg:3035')
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

def process_points(label, coords):
    conn = psycopg2.connect("dbname=gis")
    cur = conn.cursor()

    n = 0
    for (easting, northing) in coords:
        (min_x, min_y) = (easting, northing)
        (max_x, max_y) = (min_x + 10000.0, min_y + 10000.0)
        n += 1
        
        print label, "%d/%d" % (n, len(coords))
        print label, '@', easting, northing

        cur.execute("SELECT x,y FROM clc_tiling WHERE x = %s AND y = %s", (min_x, min_y))
        result = cur.fetchall()
        if len(result) > 0:
            print label, '@', easting, northing, 'skipped'
        else:
            start = time.time()
            cur.execute("INSERT INTO clc_tiled (id,code,name,geom) SELECT id,code,name,ST_Multi(ST_Intersection(geom, ST_SetSRID(ST_MakeBox2D(ST_Point(%s,%s), ST_point(%s,%s)),3035))) AS geom FROM clc06 WHERE ST_Intersects(geom, ST_SetSRID(ST_MakeBox2D(ST_Point(%s,%s), ST_Point(%s,%s)),3035))", (min_x, min_y, max_x, max_y, min_x, min_y, max_x, max_y))
            count = cur.rowcount
            cur.execute("INSERT INTO clc_tiling (x,y) VALUES(%s,%s)", (min_x, min_y))
            conn.commit()
            end = time.time()
            print label, '@', easting, northing, 'elapsed = % 2.1fs, %d row(s)' % (end - start, count)
    
    cur.close()
    conn.close()

def divid_work(source, n_workers):
    work = []
    for i in range(n_workers):
        work.append([])
    n = 0
    for item in source:
        work[n].append(item)
        n = (n + 1) % n_workers
    return work

# parameters
min_x = 3060000
min_y = 3110000
max_x = 3990000
max_y = 4280000
width = max_x - min_x
height = max_y - min_y
n_workers = os.sysconf(os.sysconf_names['SC_NPROCESSORS_ONLN'])

# bootstrap
points = grid_points(min_x, min_y, width, height, 10000.0)
work = divid_work(points, n_workers)

print "points    = ", len(points)
print "n_workers = ", n_workers

threads = []
for i in range(n_workers):
    label = "worker %d" % (i + 1)
    thread = threading.Thread(target=process_points, args=(label,work[i]))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

print 'done'
