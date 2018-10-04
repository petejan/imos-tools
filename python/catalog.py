from thredds_crawler.crawl import Crawl
import sys

if __name__ == '__main__':

    #path='ABOS/DA'
    #path='ABOS/DA/EAC2000/Temperature'
    #path='ABOS/DA/EAC2000/CTD_timeseries'
    path='ANMN/NRS/NRSKAI'
    #path='ABOS/SOTS'
    #path='ABOS/SOTS/2016'

    if len(sys.argv) > 1:
        path = sys.argv[1]

    #skips = Crawl.SKIPS + [".*FV00"]
    skips = Crawl.SKIPS + [".*FV00", ".*realtime", ".*Real-time", ".*daily", ".*REAL_TIME", ".*regridded", ".*burst", ".*gridded", ".*long-timeseries"]
    #skips = Crawl.SKIPS + [".*realtime", ".*Real-time", ".*daily", ".*REAL_TIME", ".*regridded"]
    #skips = Crawl.SKIPS + [".*regridded"]

    crawl_path = 'http://thredds.aodn.org.au/thredds/catalog/IMOS/' + path + '/catalog.xml'
    #crawl_path='http://thredds.aodn.org.au/thredds/catalog/IMOS/ANMN/NRS/NRSKAI/Biogeochem_profiles/catalog.html'

    c = Crawl(crawl_path, select=['.*FV01'], skip=skips)

    #c = Crawl('http://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/IMOS-EAC/catalog.xml', select=['.*'])
    #c = Crawl('http://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/IMOS-ITF/catalog.xml', select=['.*'])
    #c = Crawl('http://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/SOTS/catalog.xml', select=['.*'])

    #pprint.pprint(c.datasets)

    # serice can be httpService or dapService
    urls = [s.get("url") for d in c.datasets for s in d.services if s.get("service").lower() == "httpserver"]

    for url in urls:
        print(url)

    

