from datetime import datetime, UTC
import xml.etree.ElementTree as ET

# web query http://www.cmar.csiro.au/geoserver/wms?service=WFS&version=1.1.0&request=GetFeature&typeName=imos:PLANKTON_SOTS_PHYTOPLANKTON&srsName=EPSG%3A4326
# web http://www.cmar.csiro.au/geoserver/wms?service=WFS&version=1.1.0&request=GetFeature&typeName=imos:PLANKTON_SOTS_PHYTOPLANKTON&srsName=EPSG%3A4326&sortby=imos:SAMPLE_TIME
#
# get csv format
# http://www.cmar.csiro.au/geoserver/wms?service=WFS&version=1.1.0&request=GetFeature&typeName=imos:PLANKTON_SOTS_PHYTOPLANKTON&srsName=EPSG%3A4326&sortby=imos:SAMPLE_TIME&outputFormat=csv


tree = ET.parse('/Users/jan079/Desktop/features.xml')
root = tree.getroot()
for child in root:
    #print('root child', child.tag, child.attrib)

    for c in child:
        #print('child tag', c.tag)
        dep = c.find('{http://www.cmar.csiro.au/geoserver/imos}SOTS_DEPLOYMENT')
        time = c.find('{http://www.cmar.csiro.au/geoserver/imos}SAMPLE_TIME')

        family = c.find('{http://www.cmar.csiro.au/geoserver/imos}FAMILY')
        genus = c.find('{http://www.cmar.csiro.au/geoserver/imos}GENUS')
        species = c.find('{http://www.cmar.csiro.au/geoserver/imos}SPECIES')
        taxon = c.find('{http://www.cmar.csiro.au/geoserver/imos}TAXON_NAME')

        vol = c.find('{http://www.cmar.csiro.au/geoserver/imos}BIOVOLUME_UM3_PER_L')
        cpl = c.find('{http://www.cmar.csiro.au/geoserver/imos}CELL_PER_LITRE')

        caab = c.find('{http://www.cmar.csiro.au/geoserver/imos}CAAB_CODE')

        #print('deployment', dep)

        if dep is not None:
            #print('deployment text', dep.text)
            date_time = datetime.strptime(time.text, '%Y-%m-%dT%H:%MZ')
            d = dep.text + ',' + str(date_time) + ',' + taxon.text + ',' + cpl.text
            #print(dep.text, time.text, taxon.text, cpl.text)

            cb = ','
            if caab is not None:
                cb += caab.text
            v = ','
            if vol is not None:
                v += vol.text
            f = ','
            if family is not None:
                f += family.text
            g = ','
            if genus is not None:
                g += genus.text
            s = ','
            if species is not None:
                s += species.text

            print(d, cb, f, g, s)


