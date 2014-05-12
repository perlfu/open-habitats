#!/usr/bin/env python

import multiprocessing
import threading
import sys
import os

H_NONE          = 0
H_URBAN         = 10
H_WOOD          = 20
H_WETLAND       = 30
H_WATER         = 31
H_RIVER         = 32
H_GRASSLAND     = 40
H_ARABLE        = 50
H_SEMINATURAL   = 60
H_MOUNTAIN      = 70

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

habitat_types = [
    H_NONE, H_URBAN, H_WOOD, H_WETLAND,
    H_WATER, H_RIVER, H_GRASSLAND, H_ARABLE, H_SEMINATURAL,
    H_MOUNTAIN
]

classify_natural = {
    'wetland':      'wetland',
    'wood':         'wood',
    'water':        'water',
    'grassland':    'grass',
    'mud':          'mud',
    'marsh':        'marsh',
    'heath':        'heath',
    'scrub':        'scrub'
}
classify_landuse = {
    'residential':  'build',
    'commercial':   'build',
    'farm':         'agri',
    'farmland':     'agri',
    'farmyard':     'build',
    'forest':       'wood',
    'grass':        'grass',
    'industrial':   'build',
    'meadow':       'grass',
    'pasture':      'agri',
    'quarry':       'none',
    'railway':      'trans',
    'retail':       'build',
    'village_green':'grass',
    'military':     'none',
    'construction': 'build',
    'cemetery':     'build+',
    'reservoir':    'water',
    'wood':         'wood',
    'recreation_ground': 'grass',
    'allotments':   'grass'
}

def map_code_category(code, category):
    if not code:
        return H_NONE
    if category:
        category = category.strip()

    if (category == 'build') or (category == 'build+') or (category == 'trans'):
        if (code[0] == '1') or (code == '010'):
            return H_URBAN # Built-up areas and gardens
        else:
            return H_NONE
        #if (code[0] == '1') and (code[1] == '1'): # urban
        #    return H_URBAN # Built-up areas and gardens
        #else:
        #    return H_NONE
    
    #if code[0] == '1' and code[1] == '1': # urban
    #    return H_URBAN # Built-up areas and gardens
    #if code == '122' or code == '121':
    #    return H_NONE
    if (code[0] == '1') or (code == '010'):
        return H_URBAN  # Other man-made
    
    if code[0] == '3' and code[1] == '1': # forests
        return H_WOOD
    if (category == 'wood') or (code == '020'):
        return H_WOOD

    if category == 'wetland':
        return H_WETLAND
    if category == 'water':
        return H_WATER

    if (code == '231') or (code == '040'): # pastures
        return H_GRASSLAND
	   
    if category == 'grass' or category == 'scrub':
        return H_SEMINATURAL
    if category == 'mud' or category == 'marsh':
        return H_SEMINATURAL
    if category == 'heath':
        return H_MOUNTAIN
   
    if code[0] == '0': # direct translation
        return int(code[1] + code[2])

    if code[0] == '2': # arable
        return H_ARABLE # fixme

    if code == '321': # natural grasslands
        return H_SEMINATURAL
    if code[0] == '3' and code[1] == '2':
        return H_MOUNTAIN

    if code[0] == '4' and code[1] == '1': # wetlands
        return H_WETLAND

    if code == '511': # inland water course
        return H_WETLAND
    if code == '512': # inland water bodies
        return H_WATER
    
    return H_NONE

def classify_osm(data):
    if 'railway' in data:
        return 'trans'
    if 'highway' in data:
        return 'trans'
    if 'waterway' in data:
        return 'water'
    if 'aeroway' in data:
        return 'none'
    if 'wood' in data:
        return 'wood'
    if 'man_made' in data:
        return 'build'
    if 'building' in data:
        return 'build'
    if 'natural' in data:
        if data['natural'] in classify_natural:
            return classify_natural[data['natural']]
    if 'landuse' in data:
        if data['landuse'] in classify_landuse:
            return classify_landuse[data['landuse']]

    if len(data.keys()) > 3:
        print 'unclassified', data
    
    return None

def map_lcm(bh, bhsub):
    if bh == 'Arable and horticulture':
        return H_ARABLE
    if bh == 'Improved grassland':
        return H_GRASSLAND
    if bh == 'Broad leaved, mixed and yew woodland':
        return H_WOOD
    if bh == 'Built up areas and gardens':
        return H_URBAN
    if bh == 'Freshwater':
        return H_WATER
    if bh == 'Neutral grassland':
        return H_SEMINATURAL # FIXME: correct?
    if bh == 'Rough low-productivity grassland':
        return H_SEMINATURAL # FIXME: correct?
    if bh == 'Dwarf shrub heath':
        return H_MOUNTAIN # FIXME: correct?
    if bh == 'Salt water':
        return H_WATER
    if bh == 'Bog':
        return H_WETLAND
    if bh == 'Littoral sediment':
        return H_WETLAND
    if bh == 'Inland rock':
        return H_MOUNTAIN
    if bh == 'Fen marsh and swamp':
        return H_WETLAND
    if bh == 'Coniferous woodland':
        return H_WOOD
    if bh == 'Supra-littoral sediment':
        return H_WETLAND
    if bh == 'Acid grassland':
        return H_GRASSLAND # FIXME: check
    if bh == 'Calcareous grassland':
        return H_GRASSLAND # FIXME: check

    print 'unmapped broad habitat:', bh, '...', bhsub

    return H_NONE

shutdown = True

def worker_shutdown_required():
    global shutdown
    return shutdown

def do_thread_par(data, n_workers, th):
    global shutdown

    shutdown = False
    n = 0
    work = []
    for i in range(n_workers):
        work.append([])
    for p in data:
        work[n].append(p)
        n = (n + 1) % n_workers

    # execute
    workers = []
    for i in range(n_workers):
        label = "worker %d" % (i + 1)
        worker = threading.Thread(target=th, args=(label,work[i]))
        worker.start()
        workers.append(worker)

    shutdown = False
    while not shutdown:
        try:
            for worker in workers:
                worker.join()
            shutdown = True
        except:
            # force
            shutdown = True

def do_process_par(data, n_workers, ph):
    global shutdown

    shutdown = False

    n = 0
    work = []
    for i in range(n_workers):
        work.append([])
    for p in data:
        work[n].append(p)
        n = (n + 1) % n_workers

    print 'par begin...'
    # execute
    workers = []
    queues = []
    for i in range(n_workers):
        label = "worker %d" % (i + 1)
        queue = multiprocessing.Queue()
        queues.append(queue)
        worker = multiprocessing.Process(target=ph, args=(label,queue,work[i]))
        worker.start()
        workers.append(worker)

    shutdown = False
    while not shutdown:
        try:
            for worker in workers:
                worker.join()
            shutdown = True
        except:
            # force
            shutdown = True

    print 'par end...'
    results = []
    for queue in queues:
        done = False
        while not done:
            try:
                v = queue.get_nowait()
                results.append(v)
            except:
                done = True
        queue.close()
    print 'par end... complete'

    return results
