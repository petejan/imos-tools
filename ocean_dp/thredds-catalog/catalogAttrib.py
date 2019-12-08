from thredds_crawler.crawl import Crawl
from netCDF4 import Dataset
import sys

if __name__ == '__main__':

    #path='ABOS/DA'
    #path='ABOS/DA/EAC2000/Temperature'
    #path='ABOS/DA/EAC2000/CTD_timeseries'
    #path='ANMN/NRS/NRSKAI'
    #path='ABOS/SOTS'
    path='ABOS/SOTS'

    if len(sys.argv) > 1:
        path = sys.argv[1]

    #skips = Crawl.SKIPS + [".*FV00"]
    #skips = Crawl.SKIPS + [".*FV00", ".*realtime", ".*Real-time", ".*daily", ".*REAL_TIME", ".*regridded", ".*burst", ".*gridded", ".*long-timeseries"]
    skips = Crawl.SKIPS + [".*realtime", ".*Real-time", ".*daily", ".*REAL_TIME", ".*regridded", ".*burst", ".*gridded", ".*long-timeseries", ".*aggregated_timeseries"]
    #skips = Crawl.SKIPS + [".*realtime", ".*Real-time", ".*daily", ".*REAL_TIME", ".*regridded"]
    #skips = Crawl.SKIPS + [".*regridded"]

    crawl_path = 'http://thredds.aodn.org.au/thredds/catalog/IMOS/' + path + '/catalog.xml'
    #crawl_path='http://thredds.aodn.org.au/thredds/catalog/IMOS/ANMN/NRS/NRSKAI/Biogeochem_profiles/catalog.html'

    #c = Crawl(crawl_path, select=['.*FV01'], skip=skips)
    c = Crawl(crawl_path, select=['.*FV00'], skip=skips)
    #c = Crawl(crawl_path, select=['.*'], skip=skips)

    #c = Crawl('http://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/IMOS-EAC/catalog.xml', select=['.*'])
    #c = Crawl('http://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/IMOS-ITF/catalog.xml', select=['.*'])
    #c = Crawl('http://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/SOTS/catalog.xml', select=['.*'])

    # print(c.datasets)

    urls = []
    for d in c.datasets:
        #print(d)
        url = ''
        use = 0
        for s in d.services:
            if s.get("service").lower() == 'opendap':
                # for the opendap service, check at attribute value
                #print (s)
                #print (s.get("url"))
				
                nc = Dataset(s.get("url"), mode="r")
                # check for any global attributes
                #site = nc.getncattr('deployment_code')
                #print(site)

                # check for variable attributes
                var = nc.get_variables_by_attributes(standard_name='sea_water_pressure_due_to_sea_water')
                #print(var)
                if var:
                    if use == 1:
                        urls.append(s.get("url"))
                    use = 1
					
                nc.close()
				
            elif s.get("service").lower() == 'httpserver':
                # save the httpserver url for use later
                url = s.get("url")
                if use == 1:
                    urls.append(url)
                

    print('')
    for url in urls:
        print(url)

    #print()

    # serice can be httpService or dapService
    #urls = [s.get("url") for d in c.datasets for s in d.services if s.get("service").lower() == "httpserver"]  # httpserver or opendap

    #for url in urls:
    #    print(url)

    

