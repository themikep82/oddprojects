from dbhelper import *

import geoip2.database
import locale
import sys

reader = geoip2.database.Reader('GeoIP2-City.mmdb')

def main():

	runSQLFromFile('setup_queries.sql')
	
	executeSQL("SELECT * FROM unknown_ip_signups")
	
	readyFile=open('ip_data.csv', 'w')
	
	for row in cur.fetchall():
		
		try:

			response = reader.city(row[3])
			country = response.country.iso_code
			state = response.subdivisions.most_specific.iso_code

		except:
			state=None
			country=None
			
		lineToWrite='%s,%s,%s,%s,%s,%s' % (str(row[0]), str(row[1]), str(row[2]), str(row[3]), state, country)

		readyFile.write(lineToWrite)
		
		
	S3upload(readyFile.name, "/ip-temp-data/", 'analytics')	
	executeSQL("COPY ip_addresses FROM 's3://analytics/selligent_ready_files/" + readyFile.name + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ',';")

			
if __name__ == '__main__': #this goes at the very bottom of the file
	main()					#makes this py file importable into other projects