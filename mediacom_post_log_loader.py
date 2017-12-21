from settings import *
from boto.s3.key import Key
from boto.s3.connection import S3Connection
import datetime
import psycopg2
import sys

FILE_TO_READ='adspots-raw.csv'
DEBUG=0

con=psycopg2.connect(dbname=settings['database'],host=settings['host'], port=settings['port'], user=settings['user'], password=settings['password'])
cur=con.cursor()

def fillTable():
	


	rawFile = open(FILE_TO_READ, 'r')
	readyFile= open('adspots-ready.csv', 'w')
	
	#header row
	#Station 2,Date,Time,Secs, Cost ,Audience,Program,Feed,Creative,Fix or Flex,Market 2
	
	rawFile.readline() #discard header row

	for dataRow in rawFile:
		dataList=[]		
		columnIndex=0
		
		for item in dataRow.split(','):
		#at completion of this for loop, dataList will be a 0-base-index list containing each column of data 
		
				
			if columnIndex==2:
				dataList.append(fixTimestamp(item))
			if columnIndex==1:
				dataList.append(fixDate(item))
				
			else:
				dataList.append(sanitizeDataString(item))
				
			columnIndex+=1			
			
		timestamp=dataList[1]
		timestamp=timestamp[6:] + "-" + timestamp[:2] + "-" + timestamp[3:5] + " " + dataList[2] #mm-dd-yyyy converted to yyyy-mm-dd
		
		if DEBUG:
			print(timestamp)
			
		lineToWrite=dataList[0] + "," + timestamp + "," + dataList[4] + "," + dataList[5] + "," + dataList[6] + "," + dataList[7] + "," + dataList[8] + "," + dataList[9] + "," + dataList[10] + "," + dataList[11] + "," + "Mediacom" + "\n"
		readyFile.write(lineToWrite)
			
	readyFile.close()	
	
	S3upload(readyFile.name, "/tv_ad_data/", "analytics")	
	
	initTable()

	executeSQL("COPY marketing_tv_ad_data FROM 's3://analytics/tv_ad_data/adspots-ready.csv' CREDENTIALS 'aws_access_key_id=" + settings["AWS_Key"] + ";aws_secret_access_key=" + settings["AWS_Secret"] + "' CSV;")
	
	executeSQL("UPDATE marketing_tv_ad_data SET date_aired=date_aired - INTERVAL '4 hours'")
	
	cur.close()
	con.close()
		
def initTable():

	executeSQL('DROP TABLE IF EXISTS marketing_tv_ad_data;')
	
	executeSQL('CREATE TABLE IF NOT EXISTS marketing_tv_ad_data (station VARCHAR(256), date_aired TIMESTAMP, secs INTEGER, cost NUMERIC (20,10), audience NUMERIC(20,10), program VARCHAR(256), feed VARCHAR(256), creative VARCHAR(50), fix_or_flex VARCHAR(50), market VARCHAR(50), agency VARCHAR(50)) DISTKEY(creative) SORTKEY(date_aired);')

	executeSQL('GRANT SELECT ON marketing_tv_ad_data TO GROUP base_user_group;')
	
def executeSQL(query):
	#cur and con must exist as open psycopg2q redshift connection prior to calling this function
	if DEBUG:	
		print(query)
	cur.execute(query)
	con.commit()
	
def sanitizeDataString(dataString):
	
	dataString=dataString.replace("/","")
	dataString=dataString.replace("$","")
	dataString=dataString.replace("'","")
	dataString=dataString.replace(";","")
	dataString=dataString.replace("-","")
	dataString=dataString.replace("    ","")
	dataString=dataString.replace("\n","")

	return dataString

def fixDate(date):
	fixedDate=""

	for field in date.split("/"):
		if len(field)==1:
			fixedDate+="0" + field + "-"
		if len(field)==2:
			fixedDate+=field + "-"
		if len(field)==4:
			fixedDate+=field

	
	return str.replace(fixedDate, "/","-")
		
def fixTimestamp(date):
	if date.find(":")==1:
		return "0" + date
	else:
		return date
		
def S3upload(myFile, keyName, bucketString):
	'''
	following imports required:
	from boto.s3.key import Key
	from boto.s3.connection import S3Connection
	'''
	conn = S3Connection(aws_access_key_id=settings['AWS_Key'], aws_secret_access_key=settings['AWS_Secret'], host='s3.amazonaws.com')
	bucket = conn.get_bucket(bucketString)
	keyName+=myFile
	print(keyName)
	key = bucket.new_key(keyName).set_contents_from_filename(myFile)

fillTable()
