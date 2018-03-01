import pandas as pd
import numpy as np
from boto.s3.key import Key
from boto.s3.connection import S3Connection
from datetime import datetime, timedelta
import psycopg2

from settings import *

MAX_CHURN_N=50

DEBUG=1

con=psycopg2.connect(dbname=settings['database'],host=settings['host'], port=settings['port'], user=settings['user'], password=settings['password'])
cur=con.cursor()

def main():

	executeSQL('SELECT * FROM sandbox.churn_summary')
	
	resultSet = cur.fetchall()
	
	for row in resultSet:
	
		runStart=row[0]
		runEnd=row[1]
		
		executeSQL("SELECT SUM(reactivate_7_days) FROM sandbox.all_churns WHERE churn_n < %s AND churn_date BETWEEN '%s' AND '%s'" % (MAX_CHURN_N, runStart, runEnd))
		
		rd7Count=cur.fetchone()[0]
		
		if DEBUG:
			print(rd7Count)
		
		executeSQL("UPDATE sandbox.churn_summary SET actual_ra7=%s WHERE end_date='%s'" % (rd7Count, runEnd))
		
	
def executeSQL(query):
	#cur and con must exist as open psycopg2q redshift connection prior to calling this function
	if DEBUG:	
		print(query)
	cur.execute(query)
	con.commit()
	
if __name__ == '__main__':
	main()