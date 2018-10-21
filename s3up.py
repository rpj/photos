#!/usr/bin/env python

import io
import os
import re
import sys
import uuid
import threading
import Queue
import math
import time
import boto.s3
from os import stat
from getopt import getopt
from monotonic import time as mtime
from md5 import md5

def build_file_list(d):
    return map(lambda dp: dp if not os.path.isdir(dp) else build_file_list(dp), 
            map(lambda de: "{}/{}".format(d, de), os.listdir(d)))

def process_file_list(fl):
    rv = []
    for fn in fl:
        rv += process_file_list(fn) if isinstance(fn, list) else [fn]
    return rv

def send_file(path, bucket, rate):
    r_size = rate * 1024
    print "send_file('{}', '{}', {}) r_size={}".format(path, bucket, rate, r_size)
    run = True
    run_q = Queue.Queue(maxsize=1)

    fs = os.stat(path).st_size
    num_chunks = math.ceil(float(fs) / r_size)
    print "FS {} NUM_CHUNKS {} math.ceil({})".format(fs, num_chunks, (float(fs) / r_size))

    s3conn = boto.s3.connect_to_region('us-west-1')
    s3bkt = s3conn.get_bucket(bucket)
    mpup = s3bkt.initiate_multipart_upload(path)
    fuuid = uuid.uuid4()

    def read_thread():
        with open(path, "rb") as f:
            bs = f.read(r_size)
            while run and bs:
                bcs = md5(bs).hexdigest()
                print "<<Putting {} bytes (cs={}) in the queue>>".format(len(bs), bcs)
                run_q.put((io.BytesIO(bs), bcs))
                bs = f.read(r_size)

    def send_thread():
        c_cnt = 1
        while c_cnt <= num_chunks:
            s_time = mtime.time()
            print "Started chunk {} at {}".format(c_cnt, s_time)
            chunk, bcs = run_q.get()
            print "Got a chunk {} cs={}".format(chunk, bcs)
            s = mpup.upload_part_from_file(chunk, part_num=c_cnt)
            if s.size != r_size and c_cnt < num_chunks:
                raise Exception("Send size {} != chunck size {}".format(s, r_size))
            e_tag = s.etag.replace('"','')
            if e_tag != bcs:
                raise Exception("MD5 of uploaded chunk ({}) != ours ({})".format(e_tag, bcs))
            print "sent chunk, saw etag {}".format(e_tag)
            d_time = mtime.time() - s_time
            print "[{:04}] {}s -> waiting {}".format(c_cnt, d_time, 1.0 - d_time)
            time.sleep(1.0 - d_time)
            c_cnt += 1

    rt = threading.Thread(target=read_thread)
    st = threading.Thread(target=send_thread)

    print "Starting read thread"
    rt.start()
    print "Started send thread"
    st.start()

    print "Joining read thread"
    rt.join()
    print "Joining send thread"
    st.join()

    print "Done! Completeing upload"
    mpup.complete_upload()

if __name__ == "__main__":
    ops, args = getopt(sys.argv[1:], "u:c:b:")

    if not len(args):
        eprint("Usage: {} [options] search_dir".format(sys.argv[0]))
        sys.exit(0)

    ops = { k[1:]: v for k, v in ops }

    if 'b' not in ops:
        raise Exception("Must provide bucket name with -b")

    if 'u' not in ops:
        raise Exception("Must provide upload limit (in KB/s) with -u")

    bucket = ops['b']
    urate = int(ops['u'])

    print "Limiting to {}KB/s, sending objects to bucket '{}'".format(urate, bucket)

    print "Building file list..."
    fl = process_file_list(build_file_list(args[0]))
    print "{}".format(fl)
    ag_s = reduce(lambda x, y: ((stat(x).st_size if isinstance(x, str) else x) + stat(y).st_size), fl) / 1000.0
    print "AG_S {}".format(ag_s)
    ds_re = re.compile("\/\/+")
    cl = map(lambda x: re.sub(ds_re, "/", x), fl)

    print "[{}] {}".format(len(cl), cl)

    ex_mins = (ag_s / float(urate)) / 60.0
    est_mins = int(round(ex_mins + ex_mins * 0.15))
    print "Found {} files totalling {:0.1f} KB. At the specified rate, upload should take about {} minutes".format(
            len(cl), ag_s, est_mins)

    send_file(cl[0], bucket, urate)
