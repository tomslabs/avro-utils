#! /usr/bin/python
# -*- python -*-

import avro.io
import avro.datafile
import sys
import glob

try:
   import json
except ImportError:
   import simplejson as json

for f in sys.argv[1:]:
    for d in avro.datafile.DataFileReader(file(f), avro.io.DatumReader()):
        print json.dumps(d)

