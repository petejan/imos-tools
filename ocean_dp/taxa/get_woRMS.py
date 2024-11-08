from suds import null, WebFault
from suds.client import Client
import sqlite3

# add WORMS_APHIA_ID to database records from TAXON_NAME

con = sqlite3.connect('phyto.sqlite')

more_to_process = True

cl = Client('https://www.marinespecies.org/aphia.php?p=soap&wsdl=1')

while more_to_process:
    cur = con.cursor()
    names = cur.execute('SELECT oid, TAXON_NAME FROM PLANKTON_SOTS_PHYTOPLANKTON WHERE WORMS_APHIA_ID IS NULL ORDER BY SAMPLE_TIME LIMIT 50')
    #names = cur.execute('SELECT oid, TAXON_NAME FROM PLANKTON_SOTS_PHYTOPLANKTON ORDER BY SAMPLE_TIME LIMIT 100')

    names_list = []
    oid_list = []

    i = 0
    for name in names:
        print(i, name[0], name[1])
        names_list.append(name[1])
        oid_list.append(name[0])
        i = i + 1

    if i == 0:
        more_to_process = False

    scinames = cl.factory.create('scientificnames')
    scinames["_arrayType"] = "string[]"
    scinames["scientificname"] = names_list

    j = 0
    array_of_results_array = cl.service.matchAphiaRecordsByNames(scinames, like="true", fuzzy="false", marine_only="true")
    print('number of results', len(array_of_results_array))

    cur = con.cursor()
    for results_array in array_of_results_array:
        a_id = -1
        genus = None
        family = None
        for aphia_object in results_array:
    #        print(aphia_object)
            print(j, oid_list[j], 'AphiaID=%s scientific_name=%s genus=%s family=%s' % (aphia_object.AphiaID, aphia_object.scientificname, aphia_object.genus, aphia_object.family))
            a_id = aphia_object.AphiaID
            genus = aphia_object.genus
            family = aphia_object.family
        cur.execute('UPDATE PLANKTON_SOTS_PHYTOPLANKTON SET WORMS_APHIA_ID=\'%s\',GENUS=\'%s\',FAMILY=\'%s\' WHERE oid=\'%d\'' % (a_id, genus, family, oid_list[j]))
        j = j + 1

    con.commit()

con.close()
