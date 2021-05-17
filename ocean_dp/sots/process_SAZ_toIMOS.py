import sys

print('Python %s on %s' % (sys.version, sys.platform))

sys.path.extend(['.'])

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
                                                           'metadata/SAZ47.metadata.csv',
                                                           'metadata/variable.metadata.csv'])

        filename = ocean_dp.attribution.addAttributes.add(fn, ['metadata/pulse-saz-sofs-flux-timeoffset.metadata.csv'])
        filename = ocean_dp.processing.apply_scale_offset_attributes.apply_scale_offset(fn)

        filename = ocean_dp.attribution.add_geospatial_attributes.add_spatial_attr(filename)
        filename = ocean_dp.attribution.format_attributes.format_attributes(filename)

        print('step 2 (attributes) filename : ', filename)
    except RuntimeError:
        print('problem with file', fn)
        pass

    filename = ocean_dp.file_name.imosNetCDFfileName.rename(filename)
    print('step 3 imos name : ', filename)
