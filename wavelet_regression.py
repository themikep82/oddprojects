import pywt
import psycopg2
import sys
import os
from settings import *
from boto.s3.key import Key
from boto.s3.connection import S3Connection
import scipy
import numpy as np
import math

con=psycopg2.connect(dbname=settings['database'],host=settings['host'], port=settings['port'], user=settings['user'], password=settings['password'])
cur=con.cursor()

DEBUG=1

def main():

	readyFile = open('wavelet-transform.csv', 'w')
	reportFile = open('testreport.csv','w')
	dataFile = open('wavelet-data.csv','w')
	sourceFile = open('source-data.csv', 'r')
	
	path=os.path.dirname(__file__)
	
	dataList=[]
	timeStampList=[]
	executeSQL('SELECT * FROM google_analytics')

	for row in cur.fetchall():
	#row[0] is timestamp, row[1] is sessions, row[2] is entrances	
		dataList.append(float(row[2]))
		timeStampList.append(row[0])
		sourceFile.write(str(row[2]) + '\n')

	
	for row in sourceFile.readlines():
		dataList.append(float(row))

	
	for waveletType in pywt.wavelist():
		print(waveletType)

		
	for waveletType in pywt.wavelist():
		
		try:
	
		for signalExtension in pywt.Modes.modes:
		
			for decompositionLevel in range(1,13):
					
				noisy_coefs = pywt.wavedec(dataList, waveletType, level=decompositionLevel, mode=signalExtension)
				noisy_coefs=dataList
				sigma = mad(dataList)
				uthresh = float(sigma)*np.sqrt(2*np.log(len(dataList)))
				denoised = noisy_coefs[:]
				denoised[1:] = (pywt.threshold(uthresh, i, 'soft') for i in denoised[1:])
				signal = pywt.waverec(denoised, waveletType, mode=signalExtension)
				
				for dataItem in signal:
					dataFile.write(str(dataItem) + '\n')
	
				score=np.corrcoef(signal, dataList)[0][1]
							
				reportData= waveletType + ',' + signalExtension + ',' + str(decompositionLevel) + ',' + str(score) + '\n'
				
				if DEBUG:
					print( waveletType + ',' + signalExtension + ',' + str(decompositionLevel) + ',' + str(score))

				reportFile.write(reportData)

	
		except KeyboardInterrupt:
			break
							
		except:
			print(waveletType + " invalid wavelet type for pywt.wavedec")

	S3upload(readyFile.name, "/google_analytics_data/", "analytics")
	
	initTable()
	
	executeSQL("COPY ga_entrances_discrete_wavelet_transform FROM 's3://analytics/google_analytics_data/wavelet-transform.csv' CREDENTIALS 'aws_access_key_id=" + settings["AWS_Key"] + ";aws_secret_access_key=" + settings["AWS_Secret"] + "' CSV;")

def initTable():

	executeSQL('DROP TABLE IF EXISTS ga_entrances_discrete_wavelet_transform;')
	
	executeSQL('CREATE TABLE IF NOT EXISTS ga_entrances_discrete_wavelet_transform (session_date TIMESTAMP, transformed_metric NUMERIC);')

	executeSQL('GRANT SELECT ON ga_entrances_discrete_wavelet_transform TO GROUP base_user_group;')
	
def executeSQL(query):
	#cur and con must exist as open psycopg2q redshift connection prior to calling this function
	if DEBUG:
		print(query)
	cur.execute(query)
	con.commit()
	
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

def mad(arr):
    """ Median Absolute Deviation: a "Robust" version of standard deviation.
        Indices variabililty of the sample.
        https://en.wikipedia.org/wiki/Median_absolute_deviation 
    """
    arr = np.ma.array(arr).compressed() # should be faster to not use masked arrays.
    med = np.median(arr)
    return np.median(np.abs(arr - med))

if __name__ == '__main__':
	main()