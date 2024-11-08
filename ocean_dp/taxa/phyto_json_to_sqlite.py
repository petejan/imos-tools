import sqlite3
import json

con = sqlite3.connect('phyto.sqlite', detect_types=sqlite3.PARSE_DECLTYPES)

cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS PLANKTON_SOTS_PHYTOPLANKTON ("
            "FID VARCHAR(64),"
            "IMOS_SITE_CODE VARCHAR(50),"
            "SOTS_YEAR INTEGER,"
            "SOTS_DEPLOYMENT VARCHAR(50),"
            "DEPLOYMENT_VOYAGE VARCHAR(50),"
            "RETRIEVAL_VOYAGE VARCHAR(50),"
            "DEPLOYMENT_DATE VARCHAR(50),"
            "RETRIEVAL_DATE VARCHAR(50),"
            "SAMPLE_NUMBER INTEGER,"
            "SAMPLE_DATE VARCHAR(50),"
            "SAMPLE_TIME VARCHAR(50),"
            "LONGITUDE REAL,"
            "LATITUDE REAL,"
            "TAXON_NAME VARCHAR(50),"
            "FAMILY VARCHAR(50),"
            "GENUS VARCHAR(50),"
            "SPECIES VARCHAR(50),"
            "TAXON_ECO_GROUP VARCHAR(50),"
            "CAAB_CODE INTEGER,"
            "TAXON_START_DATE VARCHAR(50),"
            "CELL_PER_LITRE INTEGER,"
            "BIOVOLUME_UM3_PER_L REAL,"
            "SAMPLE_COMMENTS VARCHAR(50),"
            "STATION_PT VARCHAR(50),"
            "METHOD VARCHAR,"
            "WORMS_APHIA_ID VARCHAR);"
            )

con.commit()

# with open('PLANKTON_SOTS_PHYTOPLANKTON.json') as json_file:
#     data = json.load(json_file)
#     for f in data["features"]:
#         print(f)
#         cur.execute("INSERT INTO PLANKTON_SOTS_PHYTOPLANKTON ("
#                     "FID, "
#                     "IMOS_SITE_CODE, "
#                     "SOTS_YEAR, "
#                     "SOTS_DEPLOYMENT, "
#                     "DEPLOYMENT_VOYAGE, "
#                     "RETRIEVAL_VOYAGE, "
#                     "DEPLOYMENT_DATE, "
#                     "RETRIEVAL_DATE, "
#                     "SAMPLE_NUMBER, "
#                     "SAMPLE_DATE, "
#                     "SAMPLE_TIME, "
#                     "LONGITUDE, "
#                     "LATITUDE, "
#                     "TAXON_NAME, "
#                     "FAMILY, "
#                     "GENUS, "
#                     "SPECIES, "
#                     "TAXON_ECO_GROUP, "
#                     "CAAB_CODE, "
#                     "TAXON_START_DATE, "
#                     "CELL_PER_LITRE, "
#                     "BIOVOLUME_UM3_PER_L, "
#                     "SAMPLE_COMMENTS, "
#                     "STATION_PT, "
#                     "METHOD, "
#                     "WORMS_APHIA_ID)"
#                     "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", (
#                         f['id'],
#                         f['properties']['IMOS_SITE_CODE'],
#                         f['properties']['SOTS_YEAR'],
#                         f['properties']['SOTS_DEPLOYMENT'],
#                         f['properties']['DEPLOYMENT_VOYAGE'],
#                         f['properties']['RETRIEVAL_VOYAGE'],
#                         f['properties']['DEPLOYMENT_DATE'],
#                         f['properties']['RETRIEVAL_DATE'],
#                         f['properties']['SAMPLE_NUMBER'],
#                         f['properties']['SAMPLE_DATE'],
#                         f['properties']['SAMPLE_TIME'],
#                         f['properties']['LONGITUDE'],
#                         f['properties']['LATITUDE'],
#                         f['properties']['TAXON_NAME'],
#                         f['properties']['FAMILY'],
#                         f['properties']['GENUS'],
#                         f['properties']['SPECIES'],
#                         f['properties']['TAXON_ECO_GROUP'],
#                         f['properties']['CAAB_CODE'],
#                         f['properties']['TAXON_START_DATE'],
#                         f['properties']['CELL_PER_LITRE'],
#                         f['properties']['BIOVOLUME_UM3_PER_L'],
#                         f['properties']['SAMPLE_COMMENTS'],
#                         'POINT('+str(f['bbox'][0])+","+str(f['bbox'][1])+')',
#                         None,
#                         None,
#                     ))
#     con.commit()
#
# with open('bgc_phytoplankton_abundance_raw_data.json') as json_file:
#     data = json.load(json_file)
#     for f in data["features"]:
#         if f['properties']['Project'] == 'SOTS':
#             print()
#             print(f)
#             abd = f['properties']['abundances']
#             adb_data = json.loads(abd)
#             print(adb_data)
#             for a in adb_data:
#                 if adb_data[a] != 0:
#                     print(a, adb_data[a])
#                     cur.execute("INSERT INTO PLANKTON_SOTS_PHYTOPLANKTON ("
#                                 "FID, "
#                                 "IMOS_SITE_CODE, "
#                                 "SOTS_YEAR, "
#                                 "SOTS_DEPLOYMENT, "
#                                 "DEPLOYMENT_VOYAGE, "
#                                 "RETRIEVAL_VOYAGE, "
#                                 "DEPLOYMENT_DATE, "
#                                 "RETRIEVAL_DATE, "
#                                 "SAMPLE_NUMBER, "
#                                 "SAMPLE_DATE, "
#                                 "SAMPLE_TIME, "
#                                 "LONGITUDE, "
#                                 "LATITUDE, "
#                                 "TAXON_NAME, "
#                                 "FAMILY, "
#                                 "GENUS, "
#                                 "SPECIES, "
#                                 "TAXON_ECO_GROUP, "
#                                 "CAAB_CODE, "
#                                 "TAXON_START_DATE, "
#                                 "CELL_PER_LITRE, "
#                                 "BIOVOLUME_UM3_PER_L, "
#                                 "SAMPLE_COMMENTS, "
#                                 "STATION_PT, "
#                                 "METHOD, "
#                                 "WORMS_APHIA_ID)"
#                                 "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", (
#                                     f['id'],
#                                     f['properties']['StationCode'],
#                                     f['properties']['Year_Local'],
#                                     None,
#                                     None,  # f['properties']['DEPLOYMENT_VOYAGE'],
#                                     None,  # f['properties']['RETRIEVAL_VOYAGE'],
#                                     None,  # f['properties']['DEPLOYMENT_DATE'],
#                                     None,  # f['properties']['RETRIEVAL_DATE'],
#                                     None,  # f['properties']['SAMPLE_NUMBER'],
#                                     None,  # f['properties']['SAMPLE_DATE'],
#                                     f['properties']['SampleTime_UTC'],
#                                     f['properties']['Longitude'],
#                                     f['properties']['Latitude'],
#                                     a,
#                                     None,  # f['properties']['FAMILY'],
#                                     None,  # f['properties']['GENUS'],
#                                     None,  # f['properties']['SPECIES'],
#                                     None,  # f['properties']['TAXON_ECO_GROUP'],
#                                     None,  # f['properties']['CAAB_CODE'],
#                                     None,  # f['properties']['TAXON_START_DATE'],
#                                     adb_data[a],
#                                     None,  # f['properties']['BIOVOLUME_UM3_PER_L'],
#                                     None, # f['properties']['SAMPLE_COMMENTS'],
#                                     'POINT('+str(f['geometry']['coordinates'][0])+","+str(f['geometry']['coordinates'][1])+')',
#                                     f['properties']['Method'],
#                                     None,
#                                 ))
#     con.commit()

with open('bgc_phytoplankton_biovolume_raw_data.json') as json_file:
    data = json.load(json_file)
    for f in data["features"]:
        if f['properties']['Project'] == 'SOTS':
            print()
            print(f)
            abd = f['properties']['biovolumes']
            adb_data = json.loads(abd)
            print(adb_data)
            for a in adb_data:
                if adb_data[a] != 0:
                    print(a, adb_data[a])
                    cur.execute("UPDATE PLANKTON_SOTS_PHYTOPLANKTON SET BIOVOLUME_UM3_PER_L = ? WHERE (SAMPLE_TIME = ? AND TAXON_NAME = ?);",(adb_data[a], f['properties']['SampleTime_UTC'], a))
    con.commit()
