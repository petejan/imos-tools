import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName

import glob

import psutil
import os
import sys

process = psutil.Process(os.getpid())
print(process.memory_info().rss)  # in bytes

print('step 1 (parse)')

# for each of the new files, process them
if os.path.isfile(sys.argv[1]):
    ncFiles = [sys.argv[1]]
else:
    path = sys.argv[1] + "/"
    ncFiles = glob.glob(os.path.join(path, '*.nc'))
    print ('file path : ', path)


for fn in ncFiles:
    print ("processing " , fn)
    filename = ocean_dp.attribution.addAttributes.add(fn,
                                                      ['metadata/pulse-saz-sofs-flux.metadata.csv',
                                                       'metadata/imos.metadata.csv',
                                                       'metadata/sots.metadata.csv',
                                                       'metadata/sofs.metadata.csv',
                                                       'metadata/variable.metadata.csv'])

    filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
    filename = ocean_dp.attribution.format_attributes.format_attributes(filename)

    print('step 2 (attributes) filename : ', filename)

    #filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
    #print('step 3 imos name : ', filename)
