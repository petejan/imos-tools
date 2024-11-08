
import json
import sys

import sqlite3

with open(sys.argv[1]) as json_file:
    data = json.load(json_file)
    for f in data["features"]:
        if f['properties']['Project'] == 'SOTS':
            print(f.keys())
            print(f['properties'].keys())
