import sqlite3

import json
import sys

con = sqlite3.connect('phyto_json.sqlite', detect_types=sqlite3.PARSE_DECLTYPES)

cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS bgc_phytoplankton_abundance_raw_data (id INTEGER primary key autoincrement, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.crs' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.crs.properties' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.features' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.features.type' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.features.id' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.features.geometry_name' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.features.geometry' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.features.properties' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value VARCHAR);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.features.properties.abundances' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value REAL);")
cur.execute("CREATE TABLE IF NOT EXISTS 'bgc_phytoplankton_abundance_raw_data.features.properties.biovolumes' (id INTEGER primary key autoincrement, p_id INTEGER, name VARCHAR, value REAL);")

con.commit()

idx1 = 0
idx2 = 0
idx3 = 0
idx4 = 0


def walk_json(idx, k, obj, indent): 

    global idx1, idx2, idx3, idx4

    #print(indent, idx)
    if len(idx) <= indent:
        idx.append(0)
#    else:
#        idx[indent] += 1
    indent += 1

    if isinstance(obj, dict):
        ins = '<dict>'
    elif isinstance(obj, list):
        ins = '<list>'
    else:
        ins = obj

    if ins is None:
        pass
#        elif isinstance(obj, str) and obj.startswith("{"):
#            pass
    elif len(k) == 1:
        #cur.execute("INSERT INTO bgc_phytoplankton_abundance_raw_data (id, name, value) VALUES (?, ?, ?) RETURNING id", (idx[1], k[0], obj));
        cur.execute("INSERT INTO bgc_phytoplankton_abundance_raw_data (name, value) VALUES (?, ?) RETURNING id", (k[0], ins));
        idx1 = cur.lastrowid
    elif len(k) == 2:
        #cur.execute(f"INSERT INTO 'bgc_phytoplankton_abundance_raw_data.{k[0]}' (id, p_id, name, value) VALUES (?, ?, ?, ?) RETURNING id", (idx[2], idx[1], k[1], obj));
        cur.execute(f"INSERT INTO 'bgc_phytoplankton_abundance_raw_data.{k[0]}' (p_id, name, value) VALUES (?, ?, ?) RETURNING id", (idx1, k[1], ins));
        idx2 = cur.lastrowid
    elif len(k) == 3:
        #cur.execute(f"INSERT INTO 'bgc_phytoplankton_abundance_raw_data.{k[0]}.{k[1]}' (id, p_id, name, value) VALUES (?, ?, ?, ?) RETURNING id ", (idx[3], idx[2], k[2], obj));
        cur.execute(f"INSERT INTO 'bgc_phytoplankton_abundance_raw_data.{k[0]}.{k[1]}' (p_id, name, value) VALUES (?, ?, ?) RETURNING id ", (idx2, k[2], ins));
        idx3 = cur.lastrowid
    elif len(k) == 4:
        #cur.execute(f"INSERT INTO 'bgc_phytoplankton_abundance_raw_data.{k[0]}.{k[1]}.{k[2]}' (id, p_id, name, value) VALUES (?, ?, ?, ?) RETURNING id", (idx[4], idx[3], k[3], obj));
        cur.execute(f"INSERT INTO 'bgc_phytoplankton_abundance_raw_data.{k[0]}.{k[1]}.{k[2]}' (p_id, name, value) VALUES (?, ?, ?) RETURNING id", (idx3, k[3], ins));
        idx4 = cur.lastrowid

    new_id = cur.lastrowid
    idx[indent-1] = new_id
    print(f"Inserted row ID: {new_id}")

    returned_row = cur.fetchone()
    if returned_row:
        new_id = returned_row[0]
        print(f"Inserted product ID (using RETURNING): {new_id}")

 
    prim = False
    if isinstance(obj, dict):
        for key, value in obj.items():
            k.append(key)
            if isinstance(value, list):
                pass
                #print(f"{indent:02d} Key: {key} <list>")
            elif isinstance(value, dict):
                #print(f"{indent:02d} Key: {key} <dict>", value.keys())
                pass
            else:
                #print(f"{indent:02d} Key: {key}, Value: {value}")
                if isinstance(value, str) and value.startswith('{'):
                    bio_data = json.loads(value)
                    walk_json(idx, k, bio_data, indent)
            walk_json(idx, k, value, indent)  # Recurse for nested objects/arrays
            k.pop()
    elif isinstance(obj, list):
        for item in obj:
            print(f"{indent:02d} Item: {item}")
            walk_json(idx, k, item, indent)  # Recurse for nested objects/arrays
    else:
        # Handle primitive types (strings, numbers, booleans, null)
        print(indent, idx, f"{k} Primitive: {obj}")
        prim = True

      
    indent =- 1
    #if not prim:
    #    idx.pop()


with open(sys.argv[1], 'r') as f:
    data = json.load(f)

    walk_json([], [], data, 0)

con.commit()
