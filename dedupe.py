#!/usr/bin/env python

from __future__ import print_function
import os
import sys
import numpy
import sqlite3
import imagehash as ih
from sets import Set

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Must give path to SQLite DB file as argument")

    db_fname = sys.argv[1]
    db_extn = db_fname.split(".")[1]
    dupe_fname = os.path.basename(db_fname).split(".")[0] + ".dupes." + db_extn

    dupeconn = sqlite3.connect(dupe_fname)
    sqconn = sqlite3.connect(db_fname)

    _ex = sqconn.execute

    def exec_single(query,*args):
        return _ex(query, *args).fetchone()

    def row_by_id(rid):
        return exec_single("select * from image where id = ?", (rid,))

    cnts = {'g':0, 'h':0, 'l':0}
    h_idxs = { r[1] : r[0] for r in _ex("PRAGMA table_info(image)").fetchall() if 'hash' in r[1] }
    row_count = int(exec_single("select count(*) from image")[0])
    for i in range(1, row_count + 1):
        i_row = row_by_id(i)
        for j in range(i + 1, row_count + 1):
            j_row = row_by_id(j)
            hobjs = {hn: (ih.hex_to_hash(i_row[hi]) - ih.hex_to_hash(j_row[hi])) 
                    for hn, hi in h_idxs.items()}
            hvals = hobjs.values()
            havg = numpy.average(hvals)
            hstd = numpy.std(hvals)
            if havg <= 1:
                print("---------------------")
                print("---------------------")
                print("  GUARANTEED DUPES:")
                print("{} v {}: {} -> {}, {}".format(i_row[5], j_row[5], hobjs, havg, hstd))
                print("Row 1: {}".format(i_row))
                print("Row 2: {}".format(j_row))
                print("---------------------")
                print("---------------------")
                cnts['g'] += 1
            if havg <= 8 and havg > 1:
                print("---------------------")
                print("HIGH POTENTIAL DUPES:")
                print("{} v {}: {} -> {}, {}".format(i_row[5], j_row[5], hobjs, havg, hstd))
                print("Row 1: {}".format(i_row))
                print("Row 2: {}".format(j_row))
                print("---------------------")
                cnts['h'] += 1
            elif havg > 8 and havg < 11:
                print("Low potential dupes:")
                print("{} v {}: {} -> {}, {}".format(i_row[5], j_row[5], hobjs, havg, hstd))
                print("Row 1: {}".format(i_row))
                print("Row 2: {}".format(j_row))
                cnts['l'] += 1
    print("COUNTS: {}".format(cnts))
