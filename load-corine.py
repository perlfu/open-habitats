#!/usr/bin/env python

import subprocess
import psycopg2
import sys

if len(sys.argv) < 2:
    print "load-corine.py <clc06-file>"
    sys.exit(1)

source = sys.argv[1]

conn = psycopg2.connect("dbname=gis")
cur = conn.cursor()

p = subprocess.Popen(
    ['spatialite', source, "SELECT OGC_FID,code_06,id,AsWKT(GEOMETRY) from clc06;"],
    stdout=subprocess.PIPE
)

n = 0
for line in p.stdout:
    (_id, _code, _name, _wkt) = line.split('|', 3)
    _id = int(_id)
    r = cur.execute('insert into clc06 (id, code, name, geom) values(%s, %s, %s, ST_GeomFromText(%s, 3035))',
        (_id, _code, _name, _wkt))
    print _id, _name, _code, r

    n += 1
    if n >= 100:
        print 'commit'
        conn.commit()
        n = 0

conn.commit()
cur.close()
conn.close()
