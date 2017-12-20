from dbhelper import *

import os
import zipfile

def main():

	#initTables() #WARNING: This function will load a sql file that DROPS AND RECREATES all Selligent data tables. DO NOT UNCOMMENT UNLESS YOU ARE PREPARED TO RELOAD -ALL- SELLIGENT DATA

	wdPath = os.path.dirname(os.path.realpath(__file__))
	filesPath = wdPath + '/files/'
	
	unzipFiles(filesPath)
	
	for filename in os.listdir(filesPath):
	
		if filename.endswith('COMMUNICATIONS.csv'):
			if DEBUG:
				print(filename)
			loadCommunicationsTable(filesPath + filename)
			
		if filename.endswith('COMMUNICATIONDOMAIN.csv'):
			if DEBUG:
				print(filename)
			loadCommunicationDomainTable(filesPath + filename)
			
		if filename.endswith('MAILCLIENTCODES.csv'):
			if DEBUG:
				print(filename)
			loadMailClientCodesTable(filesPath + filename)
			
		if filename.endswith('MESSAGE.csv'):
			if DEBUG:
				print(filename)
			loadMessageTable(filesPath + filename)
			
		if filename.endswith('MESSAGEDELIVERYSTATES.csv'):
			if DEBUG:
				print(filename)
			loadMessageDeliveryStatesTable(filesPath + filename)
			
		if filename.endswith('PROBECODES.csv'):
			if DEBUG:
				print(filename)
			#loadProbeCodesTable(filesPath + filename)
			
		if filename.endswith('TRANSACTIONINFO.csv'):
			if DEBUG:
				print(filename)
			loadTransactionInfoTable(filesPath + filename)
			
		if filename.endswith('USERS_CONFIG.csv'):
			if DEBUG:
				print(filename)
			loadUsersConfigTable(filesPath + filename)
			
		if filename.endswith('INTERACTION.csv'):
			if DEBUG:
				print(filename)
			loadInteractionTable(filesPath + filename)
			
		if filename.endswith('CONTROL.csv'):
			if DEBUG:
				print(filename)
			loadControlTable(filesPath + filename)
			
		if filename.endswith('USERS.csv'):
			if DEBUG:
				print(filename)
			loadUsersTable(filesPath + filename)

def loadCommunicationsTable(filename):
	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	dataFrame['MESSAGE_TIMESTAMP'] = dataFrame['MESSAGE_TIMESTAMP'].str.replace('T',' ')
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_communications FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())
		
def loadCommunicationDomainTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	#none
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_communication_domain FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())

def loadMailClientCodesTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	#none
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_mail_client_codes FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())

def loadMessageTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	dataFrame['TEMPLATE_ID'] = dotZeroBugFix(dataFrame['TEMPLATE_ID'])
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_message FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())
	
def loadMessageDeliveryStatesTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	#none
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_message_delivery_states FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())

def loadProbeCodesTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	dataFrame['NAME']=dataFrame['NAME'].str.replace('&amp;','&')
	dataFrame['NAME']=dataFrame['NAME'].str.replace(';','')
	dataFrame['URL']=dataFrame['URL'].str.replace(';','')	
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_probe_codes FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())
	
def loadTransactionInfoTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	#none
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_transaction_info FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())
		
def loadUsersConfigTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	#none
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_users_config FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())

def loadInteractionTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	dataFrame['INTERACTION_TIMESTAMP'] = dataFrame['INTERACTION_TIMESTAMP'].str.replace('T',' ')
	dataFrame['MCS_ID'] = dotZeroBugFix(dataFrame['MCS_ID'])
	dataFrame['SENDOUT_ID'] = dotZeroBugFix(dataFrame['SENDOUT_ID'])
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_interaction FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())
		
def loadControlTable(filename):
	#EXTRACT
	readyFileName = os.path.basename(filename)
	dataFrame=pandas.read_csv(filename, sep=";")

	#TRANSFORM
	dataFrame['CONTROL_TIMESTAMP'] = dataFrame['CONTROL_TIMESTAMP'].str.replace('T',' ')
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_control FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())

def loadUsersTable(filename):

	#EXTRACT
	readyFileName = os.path.basename(filename)	
	dataFrame=pandas.read_csv(filename, sep=";", warn_bad_lines=True)

	#TRANSFORM
	dataFrame['CUSTOMERKEY_1']=dataFrame['CUSTOMERKEY_1'].str.replace(';','')
	dataFrame['CUSTOMERKEY_2']=dotZeroBugFix(dataFrame['CUSTOMERKEY_2'])
	
	#LOAD
	dataFrame.to_csv(readyFileName, index = False, sep=";")	
	S3upload(readyFileName, "/selligent_ready_files/", 'analytics')	
	executeSQL("COPY selligent_users FROM 's3://analytics/selligent_ready_files/" + readyFileName + "' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ';';")

	if DEBUG:
		print(dataFrame.head())
		
def unzipFiles(directory):

	for filename in os.listdir(directory):

		if filename.endswith('.zip'):
			zip = zipfile.ZipFile(directory + filename)
			zip.extractall(directory)
			zip.close()
			
def initTables():

	runSQLFromFile('setup_queries.sql')

def cleanUpLocalFiles(localPath, filesPath):

	for item in os.listdir(localPath):
    
		if item.endswith('.csv'):
			os.remove(item)
		
	for item in os.listdir(filesPath):
    
		if item.endswith('.csv') or item.endswith('.zip'):
			os.remove(item)
		

if __name__ == '__main__':
	main()