import sys
import psycopg2
import argparse
import time
from settings import *
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from boto.s3.key import Key
from boto.s3.connection import S3Connection

DEBUG=0 		#Set to 1 for increased verbosity for debugging purposes
PAGE_SIZE=10000 #number of results to retrieve per call to Google Core Reporting API (10,000 is max supported)
START_DATE='2016-12-20'
END_DATE='2016-12-20'
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
KEY_FILE_LOCATION = settings['GA_Key_Filename']
SERVICE_ACCOUNT_EMAIL = settings['GA_Service_Account']
VIEW_ID = '106427287' #FanDuel Parallel GTM View ID


con=psycopg2.connect(dbname=settings['database'],host=settings['host'], port=settings['port'], user=settings['user'], password=settings['password'])
cur=con.cursor()

class SampledDataError(Exception):
	pass

def initialize_analyticsreporting():
	"""Initializes an analyticsreporting service object.
	Returns:analytics an authorized analyticsreporting service object.
	"""

	credentials = ServiceAccountCredentials.from_p12_keyfile(
	SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)

	http = credentials.authorize(httplib2.Http())

	# Build the service object.
	analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)

	return analytics


def get_report(analytics, startIndex):
	# Uses the Analytics Service Object to query the Analytics Reporting API V4.
	# Use https://ga-dev-tools.appspot.com/query-explorer/ to sandbox queries
	
	return analytics.reports().batchGet(
		body={
		'reportRequests': [
		{
			'viewId': VIEW_ID,
			'samplingLevel': 'LARGE',	#avoid sampling if possible
			'pageToken': str(startIndex),
			'dateRanges': [{
							'startDate'	: START_DATE, 
							'endDate'	: END_DATE}],
			'pageSize': str(PAGE_SIZE),
			'metrics': [{'expression': 'ga:TimeOnPage'}],
			'dimensions': [	{'name':'ga:date'},
							{'name':'ga:hour'},
							{'name':'ga:minute'},
							{'name':'ga:dimension6'},
							{'name':'ga:landingPagePath'},
							{'name':'ga:exitPagePath'},
							{'name':'ga:PagePath'}]
		}]
		}
	).execute()

def writeResponseFile(response):
	readyFile = open('ga-data.dat', 'w')

	for report in response.get('reports', []):
	
		if containsSampledData(report):
			if DEBUG:
				print(reports[0].get('data', {})['rowCount'])
			raise SampledDataError
	
		for row in report.get('data', {}).get('rows', []):
			dimensions = row.get('dimensions', [])
			metrics=row.get('metrics', [])
				
			timestamp=dimensions[0][:4] + '-' + dimensions[0][4:6] + '-' + dimensions[0][6:] + ' ' + dimensions[1] + ':' + dimensions[2] + ':00' #assemble timestamp from dimension data			
				
			lineToWrite=timestamp + '|' + str(dimensions[3]) + '|' + sanitizeDataString(str(dimensions[4][:255])) + '|' + sanitizeDataString(str(dimensions[5][:255])) + '|' + sanitizeDataString(str(dimensions[6][:255])) + '|' + str(metrics[0]['values'][0]) + '\n'
			
			if DEBUG==2:
				print(lineToWrite)
			
			readyFile.write(lineToWrite)
			
		S3upload(readyFile.name, "/google_analytics_data/", "analytics")
		
		executeSQL("COPY ga_page_path FROM 's3://analytics/google_analytics_data/ga-data.dat' CREDENTIALS 'aws_access_key_id=" + settings["AWS_Key"] + ";aws_secret_access_key=" + settings["AWS_Secret"] + "';")
			
def main():

	#initTable() #WARNING: This drops and recreates the database table

	resultsTotal=0

	analytics = initialize_analyticsreporting()
	response = get_report(analytics, 1) #start on 1th object since this request returns garbage data on 0th row.

	reports = response.get('reports', [])

	if containsSampledData(reports[0]):
		raise SampledDataError	
	
	resultsTotal+=PAGE_SIZE
	maxResults=int(reports[0].get('data', {})['rowCount'])
	
	writeResponseFile(response)
	
	while resultsTotal<maxResults:
		
		try:
			
			response = get_report(analytics, int(reports[0]['nextPageToken']))
			if response.get('samplesReadCounts', []):
				raise SampledDataError
				
			reports = response.get('reports', [])
	
			resultsTotal+=PAGE_SIZE
				
			writeResponseFile(response)
				
		except KeyboardInterrupt:
			break
			
		except HttpError:
			
			if DEBUG:
				print(sys.exc_info()[0])
			time.sleep(5)	#wait 5 seconds to try again in case of HTTP error. Occasionally get 502 Bad Gateway

		
	if DEBUG:
		print(maxResults)
		print(resultsTotal)
	
def initTable():
	#Set up new table.

	executeSQL('DROP TABLE IF EXISTS ga_page_path;')
	
	executeSQL('CREATE TABLE IF NOT EXISTS ga_page_path (session_date TIMESTAMP, user_id VARCHAR(256), entry_page VARCHAR(256), exit_page VARCHAR(256), page VARCHAR(256), time_on_page NUMERIC);')

	executeSQL('GRANT SELECT ON ga_page_path TO GROUP base_user_group;')
	
def executeSQL(query):
	#cur and con must exist as open psycopg2q redshift connection prior to calling this function
	if DEBUG:
		print(query)
	cur.execute(query)
	con.commit()
			
def sanitizeDataString(dataString):
	
	dataString=dataString.strip()	#remove whitespace
	
	dataString=''.join([i if ord(i) < 128 else ' ' for i in dataString]) #filter out non utf-8 characters
	return dataString
	
def S3upload(myFile, keyName, bucketString):
	'''
	following imports required:
	from boto.s3.key import Key
	from boto.s3.connection import S3Connection
	'''
	conn = S3Connection(aws_access_key_id=settings['AWS_Key'], aws_secret_access_key=settings['AWS_Secret'], host='s3.amazonaws.com')
	bucket = conn.get_bucket(bucketString)
	keyName+=myFile
	if DEBUG:
		print(keyName)
	key = bucket.new_key(keyName).set_contents_from_filename(myFile)
	
def displayResponseMetaData(response):

	print("Row Count: " + str(response.get("rowCount")))
	print("Sample Read Counts(is data sampled): " + str(response.get("samplesReadCounts")))
	print("Data Golden?: " + str(response.get("isDataGolden")))
	
def containsSampledData(report):
	"""Determines if the report contains sampled data.

	Args:
	   report (Report): An Analytics Reporting API V4 response report.

	Returns:
	  bool: True if the report contains sampled data.
	"""
	report_data = report.get('data', {})
	sample_sizes = report_data.get('samplesReadCounts', [])
	sample_spaces = report_data.get('samplingSpaceSizes', [])
		
	if sample_sizes and sample_spaces:
		return True
		
	else:
		return False

if __name__ == '__main__':
	main()