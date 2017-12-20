import psycopg2
from settings import *

DEBUG=0

con=psycopg2.connect(dbname=settings['database'],host=settings['host'], port=settings['port'], user=settings['user'], password=settings['password'])
cur=con.cursor()

INITIAL_QUERY=''
class DataObject:

	def __init__(self, userIDArg):
	
		self.featuresData={}
		self.featuresData['user_id']=userIDArg

		self.reportDataCollected=False

	def getUserID(self):
		return self.featuresData['user_id']
	
	def __eq__(self, user2):	#required for if not in statements to work
		return self.featuresData['user_id']==user2.getUserID()
	
	def getReportLineCSV(self):	
	
		reportLine=''
	
		for key in self.featuresData:
			
			reportLine+=str(self.featuresData[key]) + ','
			
		reportLine=reportLine[:-1] + '\n'
		
		return reportLine

	def getCSVHeader(self):
		
		headerString=''
	
		for key in self.featuresData:
			
			headerString+=key + ','
			
		headerString=headerString[:-1] + '\n'
		
		return headerString
		
	def collectFeaturesFromRedshift(self):
	
		'''
			use self.__getFeature() here to collect each machine learning feature. They will be added to a master dictionary
		''' 
		
		self.reportDataCollected=True
		
		
	def __getFeature(self, dictKey, queryString):
		
		try:
			executeSQL(queryString)
			self.featuresData[dictKey]=cur.fetchone()[0]
		
		except:
			self.featuresData[dictKey]=''
			
def main():

	userList=[]
	
	count=0
	
	executeSQL(INITIAL_QUERY)
	
	queryResults=cur.fetchall()
	
	try:
	
		for queryData in queryResults:
		
			newUser=DataObject(queryData[0], queryData[1], queryData[2])
			newUser.collectFeaturesFromRedshift()
			userList.append(newUser)
			count+=1
			print(count)
	
	except:
	
		pass
		
	createDataFile(userList)
		
def executeSQL(query):
	#cur and con must exist as open psycopg2q redshift connection prior to calling this function
	if DEBUG:
		print(query)
	cur.execute(query)
	con.commit()

def sanitizeDataString(dataString):
	
	dataString=dataString.strip()	#remove whitespace	
	dataString=dataString.strip('\n')
	dataString=dataString.strip(',')
	dataString=''.join([i if ord(i) < 128 else ' ' for i in dataString]) #filter out non utf-8 characters
	return dataString

def createDataFile(dataList):

	writeFile=open('ml_feature_data.csv', 'w')
			
	firstPass=True
		
	for user in dataList:
	
		if firstPass:
			writeFile.write(user.getCSVHeader())
			firstPass=False

		writeFile.write(user.getReportLineCSV())
		
if __name__ == '__main__':
	main()