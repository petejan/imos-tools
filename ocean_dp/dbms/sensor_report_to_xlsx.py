
import pandas as pd
import psycopg2

# open a database connection
conn = psycopg2.connect(host="localhost", database="IMOS-DEPLOY", user="pete", password="password")
cur = conn.cursor()

dat = pd.read_sql_query("SELECT * FROM sensor_report_data", conn)

#print(dat)

dat.to_excel("sots-instrument-deployments.xlsx", index=False, freeze_panes=(1, 4))

conn.close()
