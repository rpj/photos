#!/usr/bin/python -u

from __future__ import print_function
import os
import sys
import json
import sqlite3
import imagehash as ih
from PIL import Image, ImageStat, ExifTags, TiffTags
from getopt import getopt

# enable 'whash' at your own risk: it can be *incredibly* slow
H_ALGS = [ih.average_hash, ih.phash, ih.dhash]#, ih.whash]

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def hashes_for_image(img):
    rd = {}
    for f in H_ALGS:
        rd[str(f.__name__)] = str(f(img))
    return rd

def build_file_list(d):
    return map(lambda dp: dp if not os.path.isdir(dp) else build_file_list(dp), 
            map(lambda de: "{}/{}".format(d, de), os.listdir(d)))

def process_file(fn):
    if fn:
        try:
            img = Image.open(fn)
            return [fn]
        except IOError as ioe:
            pass

    return []

def process_file_list(fl):
    rv = []
    for fn in fl:
        rv += process_file_list(fn) if isinstance(fn, list) else process_file(fn)
    return rv

def get_exif(img):
    if hasattr(img, '_getexif') and hasattr(img._getexif(), "items"):
        return { ExifTags.TAGS[k]: v for k, v in img._getexif().items() if k in ExifTags.TAGS }
    elif hasattr(img, 'tag'):
        return { TiffTags.TAGS[k]: v for k, v in img.tag.items() if k in TiffTags.TAGS }
    return {}

def get_image_stats(img):
    include = ['rms', 'sum', 'sum2', 'mean']
    stat = ImageStat.Stat(img)
    return { k: getattr(stat, '_get' + k)() for k in include if '_get' + k in dir(stat) }

def process_image_list(il, sinks=None):
    stats = {'formats':{},'modes':{},'pixels':{},'have_exif':0,'processed':0}
    sinks = sinks if sinks else []

    def proc_wrapper(iname):
        i = Image.open(iname)
        ex = get_exif(i)

        if 'MakerNote' in ex:
            del ex['MakerNote']

        _p = str(i.width * i.height)
        stats['pixels'][_p] = stats['pixels'][_p] + 1 if _p in stats['pixels'] else 1
        stats['formats'][i.format] = stats['formats'][i.format] + 1 if i.format in stats['formats'] else 1
        stats['modes'][i.mode] = stats['modes'][i.mode] + 1 if i.mode in stats['modes'] else 1
        stats['processed'] += 1

        if len(ex):
            stats['have_exif'] += 1

        pimg = {
            'path': iname,
            'hashes': hashes_for_image(i),
            'format': i.format,
            'mode': i.mode,
            'width': i.width,
            'height': i.height,
            'exif': ex,
            'stats': get_image_stats(i)
        }

        map(lambda s: s.sinkProcessedImage(pimg), sinks)

    # use 'map' whenever possible: it's automatically parallelized!
    map(lambda s: s.preprocess(len(il)), sinks)
    map(lambda iname: proc_wrapper(iname), il)
    map(lambda s: s.postprocess(), sinks)

    return stats

class BaseImageSink(object):
    # called before any images are sunk, 'num' being how many are waiting to be processed
    def preprocess(self, num):
        pass

    # called after all images have been sunk
    def postprocess(self):
        pass

    # called for each image to be sunk
    def sinkProcessedImage(self, pimg):
        pass

# basic class that just prints a json-ish array to stdout
class ProcImageSink(BaseImageSink):
    def preprocess(self, num):
        print("[")

    def postprocess(self):
        print("]")

    def sinkProcessedImage(self, pimg):
        print(json.dumps(pimg) + ",")

class StatusSink(BaseImageSink):
    def __init__(self):
        self._count = 1
        self._ex = 1

    def preprocess(self, num):
        self._ex = num

    def postprocess(self):
        eprint("Finished processing {} images.".format(self._ex))

    def sinkProcessedImage(self, p):
        eprint("[{:6.2f}%, {:05}/{:05}] {}".format((float(self._count) / self._ex) * 100, self._count, self._ex, p['path']))
        self._count += 1

class SQLiteSink(BaseImageSink):
    def __init__(self, path=None):
        if not path:
            raise BaseException("SQLiteSink needs a path!")

        self._path = path
        self._errcnt = {'insert_image':0,'insert_exif':0,'commit':0,'json_encode':0}

    def preprocess(self, num):
        if os.path.exists(self._path):
            eprint("SQL sink removing existing DB at '{}'".format(self._path))
            os.remove(self._path)

        self._conn = sqlite3.connect(self._path)
        self._conn.text_factory = lambda x: unicode(x, "utf-8", "ignore")

        _cur = self._conn.cursor()

        _cur.execute("create table image_exif (id integer primary key, img_id integer, make text, model text, digi_time real, bulk_json text)")
        _cur.execute("create table image (id integer primary key, width integer, height integer, format text, name text, ahash text, phash text, dhash text)")

        self._conn.commit()

    def postprocess(self):
        eprint("\nSQL sink (to '{}') error report:\n{}\n".format(self._path, json.dumps(self._errcnt)))

    def sinkProcessedImage(self, p):
        _c = self._conn.cursor()
        a = (p['width'], p['height'], p['format'], os.path.split(p['path'])[-1], p['hashes']['average_hash'], p['hashes']['phash'], p['hashes']['dhash'])
        iid = -1

        try:
            iid = _c.execute("insert into image values(NULL, ?, ?, ?, ?, ?, ?, ?)", a).lastrowid
        except Exception as e:
            eprint("Failed to insert: '{}'".format(e))
            eprint("SQL args:\n{}".format(a))
            self._errcnt['insert_image'] += 1

        if 'exif' in p and iid > 0:
            e = {k: list(v) if isinstance(v, tuple) else v for k, v in p['exif'].items()}
            mk = e['Make'] if 'Make' in e else None
            md = e['Model'] if 'Model' in e else None
            dt = e['DateTimeDigitized'] if 'DateTimeDigitized' in e else None

            # fixup for images for which each EXIF field is represented as a list
            # won't modify the values if they are already a scalar type
            af = map(lambda x: x[0] if isinstance(x, list) else x, [mk, md, dt])

            a = (iid, af[0], af[1], af[2], None)
            try:
                e_enc = json.dumps(e).encode('utf-8')
                a = (iid, af[0], af[1], af[2], e_enc)
            except Exception as _e:
                eprint("Failed to encode EXIF as JSON: '{}'".format(_e))
                eprint("EXIF in question:\n{}".format(e))
                self._errcnt['json_encode'] += 1

            try:
                _c.execute("insert into image_exif values(NULL, ?, ?, ?, ?, ?)", a)
            except Exception as _e:
                eprint("Failed to insert EXIF: '{}'".format(_e))
                eprint("JSON'ed EXIF:\n{}".format(a[4]))
                eprint("SQL args:\n{}".format(a))
                self._errcnt['insert_exif'] += 1

        if iid > 0:
            try: 
                self._conn.commit()
            except Exception as _e:
                eprint("Failed to commit SQL transaction: '{}'".format(_e))
                self._errcnt['commit'] += 1
 
if __name__ == "__main__":
    ops, args = getopt(sys.argv[1:], "u:s:")

    if not len(args):
        eprint("Usage: {} [options] search_dir".format(sys.argv[0]))
        sys.exit(0)

    sinks = []
    ops = { k[1:]: v for k, v in ops }

    if 's' in ops:
        sinks.append(SQLiteSink(ops['s']))

    # add the status sink only if another has been added, else use the default ProcImageSink
    sinks.append(StatusSink() if len(sinks) else ProcImageSink())

    eprint("Building image list...")
    ilist = process_file_list(build_file_list(args[0]))
    eprint("\t... found {} images to process.".format(len(ilist)))
    all_stats = process_image_list(ilist, sinks)
    eprint("\nAggregate statistics:\n{}".format(json.dumps(all_stats)))
