import sys
import os

print('Python %s on %s' % (sys.version, sys.platform))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), '..'))

import ocean_dp.attribution.addAttributes
import ocean_dp.attribution.add_geospatial_attributes
import ocean_dp.file_name.site_instrument
import ocean_dp.attribution.format_attributes
import ocean_dp.file_name.imosNetCDFfileName
import ocean_dp.processing.apply_scale_offset_attributes

import glob

import os
import sys

ncFiles = []
for f in sys.argv[1:]:
    ncFiles.extend(glob.glob(f))

for fn in ncFiles:
    print('step 1 (parse)')

    print ("processing " , fn)

    # need to apply any timeoffset first
    try:
        filename = ocean_dp.attribution.addAttributes.add(fn,
                                                          ['metadata/pulse-saz-sofs-flux.metadata.csv',
                                                           'metadata/imos.metadata.csv',
                                                           'metadata/sots.metadata.csv',
                                                           'metadata/sofs.metadata.csv',
                                                           'metadata/asimet.metadata.csv',
                                                           'metadata/variable.metadata.csv'])

        filename = ocean_dp.attribution.addAttributes.add(fn, ['metadata/pulse-saz-sofs-flux-timeoffset.metadata.csv'])
        print('file-name', filename)
        filename = ocean_dp.processing.apply_scale_offset_attributes.apply_scale_offset([filename])

        filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename[0])
        filename = ocean_dp.attribution.format_attributes.format_attributes(filename)

        print('step 2 (attributes) filename : ', filename)
    except RuntimeError as ex:
        print('problem with file', fn)
        print(ex)
        pass

    filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
    print('step 3 imos name : ', filename)
