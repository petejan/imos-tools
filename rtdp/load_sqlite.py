import csv, sqlite3
import sys

con = sqlite3.connect("imei-deployments.db") # change to 'sqlite:///your_filename.db'
cur = con.cursor()
cur.execute("CREATE TABLE imei (imei, deployment_date, recovery_date, deployment_code, processor);") # use your column names here

with open(sys.argv[1],'r') as fin: # `with` statement available in 2.5+
    # csv.DictReader uses first line in file for column headings by default
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['imei'], i['deployment_date'], i['recovery_date'], i['deployment'], i['processor']) for i in dr]

cur.executemany("INSERT INTO imei (imei, deployment_date, recovery_date, deployment_code, processor) VALUES (?, ?, ?, ?, ?);", to_db)
con.commit()
con.close()