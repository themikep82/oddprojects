from __future__ import print_function
import httplib2
import os
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

import psycopg2
from settings import *

DEBUG=1

con=psycopg2.connect(dbname=settings['database'],host=settings['host'], port=settings['port'], user=settings['user'], password=settings['password'])
cur=con.cursor()

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'
SPREADSHEET_ID='ENTER_GOOGLE_SHEET_ID_HERE'
GAME_IDs=[18580,18591,18586,18617]


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def main():

	credentials = get_credentials()
	http = credentials.authorize(httplib2.Http())
	discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
					'version=v4')
	service = discovery.build('sheets', 'v4', http=http,
					discoveryServiceUrl=discoveryUrl)
					
	dayNum=0			
		
	for game in GAME_IDs:
		dayNum+=1
		getFantasyScores(game, 'day%sscores!A1' % str(dayNum), service)
		    
def executeSQL(query):
	#cur and con must exist as open psycopg2q redshift connection prior to calling this function
	if DEBUG:
		print(query)
	cur.execute(query)
	con.commit()

def formatResultsForSheets(queryResults):

	formattedResults=[]

	for row in queryResults:	
		
		formattedRow=[]
		formattedRow.append(row[0])
		formattedRow.append(row[1])
		formattedRow.append(str(row[2]))
		
		formattedResults.append(formattedRow)

	if DEBUG:
		print(formattedResults)		
	return formattedResults
	
def getFantasyScores(gameID, rangeName, service):
	
		executeSQL('SELECT game_id, player_id, final_score FROM player_scores WHERE game_id=%s AND final_score IS NOT NULL' % gameID)

		body = {
		'values': formatResultsForSheets(cur.fetchall())
		}

		result = service.spreadsheets().values().append(
		spreadsheetId=SPREADSHEET_ID, range=rangeName,
		valueInputOption='USER_ENTERED', body=body).execute()
	
if __name__ == '__main__':
    main()