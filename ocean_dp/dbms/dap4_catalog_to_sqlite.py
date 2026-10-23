import os
import sqlite3
from datetime import datetime, UTC

import dateutil
from pydap.client import open_url

import sys

from thredds_crawler.crawl import Crawl

def getAttributeOrDefault(dataset, name, default):
    try:
        value = dataset.getncattr(name)
    except (AttributeError):
        value = default

    print("get attribute", name, value)
    return value


def getDateOrDefault(dataset, name, default):
    value_att = getAttributeOrDefault(dataset, name, default)
    if value_att is None:
        value = None
    else:
        value = dateutil.parser.parse(value_att)

    return value


def sqlite_insert(con, id, dap4):

    print('file-name', id)

    pyds = open_url(dap4, protocol='dap4')

    # Inspect variables and tree structure lazily (no heavy download yet)
    for a in pyds.attributes:
        print(a, pyds.attributes[a])

    # title = getAttributeOrDefault(nc, 'title', None)
    #
    # site_code = getAttributeOrDefault(nc, 'site_code', None)
    # platform_code = getAttributeOrDefault(nc, 'platform_code', None)
    deployment_code = pyds.attributes['deployment_code']
    model = pyds.attributes['instrument_model']
    sn = pyds.attributes['instrument_serial_number']
    depth = pyds.attributes['instrument_nominal_depth']
    fv = pyds.attributes['file_version']
    # featureType = getAttributeOrDefault(nc, 'featureType', None)
    #
    # geospatial_lat_min = float(getAttributeOrDefault(nc, 'geospatial_lat_min', None))
    # geospatial_lon_min = float(getAttributeOrDefault(nc, 'geospatial_lon_min', None))
    # geospatial_lat_max = float(getAttributeOrDefault(nc, 'geospatial_lat_max', None))
    # geospatial_lon_max = float(getAttributeOrDefault(nc, 'geospatial_lon_max', None))
    # geospatial_vertical_min = float(getAttributeOrDefault(nc, 'geospatial_vertical_min', None))
    # geospatial_vertical_max = float(getAttributeOrDefault(nc, 'geospatial_vertical_max', None))
    #
    date_created = pyds.attributes['date_created']

    time_deployment_start = pyds.attributes['time_deployment_start']
    time_deployment_end = pyds.attributes['time_deployment_end']
    #
    # principal_investigator = getAttributeOrDefault(nc, 'principal_investigator', None)
    # print('file_name ', os.path.basename(http))
    #
    # cur.execute("INSERT INTO file (name, http, opendap, title, site_code, platform_code, deployment_code, featuretype, "
    #             "geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min, "
    #             "geospatial_lat_max, geospatial_lon_max, geospatial_vertical_max, "
    #             "date_created, time_deployment_start, time_deployment_end, principal_investigator)"
    #             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ",
    #             (os.path.basename(http), http, opendap, title, site_code, platform_code, deployment_code, featureType,
    #              geospatial_lat_min, geospatial_lon_min, geospatial_vertical_min,
    #              geospatial_lat_max, geospatial_lon_max, geospatial_vertical_max,
    #              date_created, time_deployment_start, time_deployment_end, principal_investigator))
    # con.commit()
    cur.execute("INSERT INTO file (name, http, dap4, deployment_code, "
                "date_created, time_deployment_start, time_deployment_end, "
                "instrument_model, instrument_serial_number, instrument_nominal_depth, file_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ",
                (id, '', dap4, deployment_code,
                 date_created, time_deployment_start, time_deployment_end,
                 model, sn, depth, fv))
    con.commit()

if __name__ == "__main__":

    if (len(sys.argv) > 1):
        path = sys.argv[1]
        if not path.endswith('/'):
            path = path + '/'
    else:
        path = ''

    print('path', path)

    skips = Crawl.SKIPS + [".*realtime", ".*Real-time", ".*daily", ".*REAL_TIME", ".*regridded", ".*burst", ".*gridded", ".*long-timeseries"]

    crawl_path = 'http://thredds.aodn.org.au/thredds/catalog/IMOS/DWM/SOTS/'+path+'catalog.xml'

    c = Crawl(crawl_path, select=['.*'], skip=skips, debug=True)

    # create a list of urls to catalog, save the opendap url and the http links
    urls = []
    for d in c.datasets:
        print (d.name, d.id)
        dap4 = 'https://thredds.aodn.org.au/thredds/dap4/' + d.id

        urls.append({'name': d.name, 'id': id, 'dap4': dap4})

    now = datetime.now(UTC)
    dbname = 'thredds-' + now.strftime("%Y-%m-%d") + '.sqlite'

    con = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
    #con = psycopg2.connect(host='localhost', dbname='threddscat', user='postgres', password='secret')

    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS file (file_id integer primary key autoincrement, name TEXT, http TEXT, dap4 TEXT"
                ", title TEXT, site_code TEXT, platform_code TEXT, deployment_code TEXT, featuretype TEXT"
                ", geospatial_lat_min REAL, geospatial_lon_min REAL, geospatial_vertical_min REAL"
                ", geospatial_lat_max REAL, geospatial_lon_max REAL, geospatial_vertical_max REAL"
                ", file_version TEXT, date_created TEXT, time_deployment_start TEXT, time_deployment_end TEXT"
                ", principal_investigator TEXT "
                ", instrument_model TEXT, instrument_serial_number TEXT"
                ", instrument_nominal_depth TEXT"
                ")")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS global_attributes (file_id integer, name TEXT, type TEXT, value TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS variables (file_id integer, name TEXT, standard_name TEXT, long_name TEXT, units TEXT, aux_vars TEXT, dimensions TEXT, type TEXT)")
    con.commit()
    cur.execute("CREATE TABLE IF NOT EXISTS variable_attributes (file_id integer, var_name TEXT, name TEXT, type TEXT, value TEXT)")
    con.commit()
    # cur.execute("CREATE VIEW IF NOT EXISTS file_deployment AS "
    #             "SELECT file.file_id, file.name, dep.value AS deployment_code, nd.value AS nominal_depth "
    #             "FROM file"
    #             " LEFT join attributes AS dep ON (file.file_id = dep.file_id and dep.name == 'deployment_code')"
    #             " LEFT join attributes AS nd ON (file.file_id = nd.file_id and nd.name == 'instrument_nominal_depth')")
    # cur.execute("CREATE VIEW IF NOT EXISTS variable_depth AS "
    #             "SELECT file.file_id, file.name AS file_name, variables.name, variables.dimensions, variables.type, variables.data, nd.value AS nominal_depth "
    #             "FROM variables"
    #             " JOIN file ON (file.file_id = variables.file_id)"
    #             " LEFT join attributes AS nd ON (file.file_id = nd.file_id and nd.name == 'instrument_nominal_depth')")
    # cur.execute("CREATE VIEW IF NOT EXISTS file_instrument AS select f.file_id , f.name, m.value as model, s.value as sn FROM file f "
    #             "LEFT JOIN attributes m ON (f.file_id = m.file_id and m.name = 'instrument') "
    #             "LEFT JOIN attributes s ON (s.file_id = f.file_id and s.name = 'instrument_serial_number')")
    con.commit()

    for url in urls:
        print(url)
        print(os.path.basename(url['dap4']))
        sqlite_insert(con, url['name'], url['dap4'])

    con.close()
