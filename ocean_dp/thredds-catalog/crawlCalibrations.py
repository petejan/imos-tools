import psycopg2


conn = None
try:
    conn = psycopg2.connect(host="localhost", database="IMOS-DEPLOY", user="jan079", password="password")
    cur = conn.cursor()
    cur.execute("select cmdddid, cmdddname, cmdidid, cmdidmodel, cmdidserialnumber from cmddeploymentdetails join cmditemlink on (cmdddid = cmdil_ddid)	join cmditemdetail on (cmdil_childidid = cmdidid)  where cmdddname like 'EAC%2019' and cmdidmodel like 'SBE37%'  order by cmdddname, cmdidmodel, cmdidserialnumber")

    rows = cur.fetchall()
    print("The number of instruments: ", cur.rowcount)
    for row in rows:
        print(row)
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None:
        conn.close()

