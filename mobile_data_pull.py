import hashlib
import psycopg2
from settings import * #settings.py contains a dictionary storing my personal redshift and AWS credentials

DEBUG=0		#set to 1 for increased verbosity

QUERY_STRING='select user data sql here'


con=psycopg2.connect(dbname=settings['database'],host=settings['host'], port=settings['port'], user=settings['user'], password=settings['password'])
cur=con.cursor()

def main():

	executeSQL(QUERY_STRING)
	
	dataFile= open('mobile_user_data.csv', 'w')
	
	for row in cur:

		#row[0] is email address
		#row[1] is mobile ad id
		#row[2] is ad id type
		#row[3] is ip address
		#row[4] is Unix epoch timestamp
		#row[5] is Opt in/out
		
		lineToWrite=createHashMD5(row[0]) + ','
		lineToWrite+=createHashSHA1(row[0]) + ','
		lineToWrite+=createHashSHA256(row[0]) + ','
		lineToWrite+=row[1] + ','
		lineToWrite+=row[2] + ','
		lineToWrite+=row[3] + ','
		lineToWrite+=str(row[4]) + ','
		lineToWrite+=row[5] + '\n'
		
		if DEBUG:
			print(lineToWrite)
			
		dataFile.write(lineToWrite)
		
	dataFile.close()
	
	cur.close()
	con.close()
	
def executeSQL(query):
	#cur and con must exist as open psycopg2q redshift connection prior to calling this function
	if DEBUG:
		print(query)
	cur.execute(query)
	con.commit()
	
def createHashMD5(dataString):
	return hashlib.md5(str.encode(dataString)).hexdigest()

def createHashSHA1(dataString):
	return hashlib.sha1(str.encode(dataString)).hexdigest()

def createHashSHA256(dataString):
	return hashlib.sha256(str.encode(dataString)).hexdigest()
	
if __name__ == '__main__':
	main()
