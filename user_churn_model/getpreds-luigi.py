import pandas as pd
import numpy as np
import luigi
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
from sklearn import tree
from sklearn import linear_model
from sklearn import ensemble
from sklearn.linear_model import LinearRegression
from sklearn import preprocessing
from sklearn.decomposition import PCA
from scipy.stats import boxcox
from boto.s3.key import Key
from boto.s3.connection import S3Connection
from datetime import datetime, timedelta
from dbhelper import *
from credentials import *
import psycopg2
import csv
from credentials import *

DEBUG=1

MAX_churn_N=50

OUTPUT_COLUMNS=['user_id', 'churn_n', 'ra7_glm_preds', 'ra7_rf_preds', 'ra7_gbm_preds', 'ra7_avg_preds', 'model_date']

RANDOM_FOREST_PARAMS={"max_depth"		: 10,
					 "max_features"		: 10,
					 "min_samples_leaf"	: 10,
					 "min_samples_split": 20,
					 "n_jobs"			: 4,
					 "n_estimators"		: 200}

GBM_PARAM_GRID={"max_depth"			: [10, 15, 20, 30],
				"max_features"		: [10, 15, 20],
				"min_samples_split"	: [10, 15, 20],
				"min_samples_leaf"	: [10, 15, 20],
                "n_estimators"		: range(20,81,10)}
				
		
TIMES={	'todaysDate'	: datetime.today().strftime('%m-%d-%Y'),
		'twoWeekAgo'	: (datetime.today() - timedelta(days=14)).strftime('%m-%d-%Y'),
		'lastYearStart'	: (datetime.today() - timedelta(days=379)).strftime('%m-%d-%Y'),
		'lastYearEnd'	: (datetime.today() - timedelta(days=365)).strftime('%m-%d-%Y'),
		'twoYearStart'	: (datetime.today() - timedelta(days=745)).strftime('%m-%d-%Y'),
		'twoYearEnd'	: (datetime.today() - timedelta(days=731)).strftime('%m-%d-%Y'),
		'currentMonth'	: datetime.now().month,
		'currentYear'	: datetime.now().year,
		'lastYear'		: datetime.now().year - 1,
		'twoYearsAgo'	: datetime.now().year - 2}

con=psycopg2.connect(dbname=credentials['database'],host=credentials['host'], port=credentials['port'], user=credentials['user'], password=credentials['password'])
cur=con.cursor()

#def main():

	#updatechurnTable()

	#testData=collectTestData()
	
	#trainData=collectTrainData()
	
	#features=getFeatureList(trainData)
	
	#trainData=trainData.dropna(subset=features)
	
	#testData=testData.dropna(subset=features)
	
	#testData['ra7_glm_preds'] = runGLMPredections(trainData, testData, features)[:,1]
	
	#testData['ra7_rf_preds'] = runRandomForestPredictions(trainData, testData, features)[:,1]
		
	#testData['ra7_gbm_preds'] = runGBMPredictions(trainData, testData, features)[:,1]
	
	#testData['ra7_avg_preds'] = (testData['ra7_rf_preds'] + testData['ra7_glm_preds'] + testData['ra7_gbm_preds']) / 3

	#if DEBUG:
	
	#	print(testData.shape[0])
	
	#	print('verifying AUCs:')
		
	#	print('random forest')
	#	print(metrics.roc_auc_score(y_score=testData['ra7_rf_preds'], y_true=testData['reactivate_7_days']))
    
	#	print('glm')
	#	print(metrics.roc_auc_score(y_score=testData['ra7_glm_preds'], y_true=testData['reactivate_7_days']))

	#	print('gbm')
	#	print(metrics.roc_auc_score(y_score=testData['ra7_gbm_preds'], y_true=testData['reactivate_7_days']))
	#	
	#	print('avg')
	#	print(metrics.roc_auc_score(y_score=testData['ra7_avg_preds'], y_true=testData['reactivate_7_days']))
			
	#writePreds(testData)
		
class GetFeatureList(luigi.Task):

	def requires(self):
		return CollectTrainData

	def output(self):
		return features
		
	def run(self):
			
		features=list(dataFrame.columns)
		features.remove('user_id')
		features.remove('previous_deposit_date')
		features.remove('churn_date')
		features.remove('reactivate_date')
		#features.remove('churn_n')
		features.remove('reactivate')
		features.remove('reg_date')
		features.remove('reactivate_amount')
		#features.remove('fdp_balance')
		features.remove('reactivate_7_days')
		features.remove('reactivate_14_days')
		features.remove('reactivate_30_days')
	
class UpdatechurnTable(luigi.Task):

    def output(self):
        return luigi.LocalTarget('updatedchurns')

    def run(self):
		
		if DEBUG:
			print('Updating churn Events Table...')

		queryFileData=''
		
		with open('setup_queries.sql', 'r') as queryFile:
			queryFileData+=queryFile.read()
		
		allQueries=queryFileData.split(';')
		
		for query in allQueries:
			if query!='':
				executeSQL(query)

class CollectTrainData(luigi.Task):

	def requires(self):
		return UpdatechurnTable

	def output(self):
		return luigi.LocalTarget('churn_train_all.csv')
		
	def run(self):

		if DEBUG:
			print('Collecting Training Data...')

		query = "SELECT * FROM all_churns WHERE ((churn_date BETWEEN '%s' AND '%s') OR (churn_date BETWEEN '%s' AND '%s')) AND churn_n<=%s" % (TIMES['lastYearStart'], TIMES['lastYearEnd'], TIMES['twoYearStart'], TIMES['twoYearEnd'], MAX_churn_N)
		executeSQL(query)

		colnames = [desc[0] for desc in cur.description]

		writeFile=open('churn_train_all.csv','w')
		writer=csv.writer(writeFile)
		writer.writerow(colnames)

		for result in cur.fetchall():
			writer.writerow(result)

		return pd.read_csv('churn_train_all.csv')
	
class CollectTestData(luigi.Task):

	def requires(self):
		return updatechurnTable

	def output(self):
		return luigi.LocalTarget('churn_test_all.csv')
		
	def run(self):
		pd.read_csv('churn_test_all.csv')
		
		if DEBUG:
			print('Collecting Test Data...')
			
		query = "SELECT * FROM all_churns WHERE churn_date BETWEEN '%s' AND '%s' AND churn_n<=%s" % (TIMES['twoWeekAgo'], TIMES['todaysDate'], MAX_churn_N)

		executeSQL(query)

		colnames = [desc[0] for desc in cur.description]

		writeFile=open('churn_test_all.csv','w')
		writer=csv.writer(writeFile)
		writer.writerow(colnames)

		for result in cur.fetchall():
			writer.writerow(result)

class PrepDataForGLM(luigi.Task):

	def requires(self):
		return CollectTrainData

	def output(self):
		return luigi.LocalTarget(readyData)
		
	def run(self):

		if DEBUG:
			print('Normalizing Data for GLM...')

		readyData=boxcox(dataFrame) #boxcox

		scaler=preprocessing.StandardScaler().fit(dataFrame)
		readyData=scaler.transform(dataFrame)

		pca=PCA(n_components=20) #pca
		pca.fit(readyData)

		readyData=pca.transform(readyData)

class RunRandomForestPredictions(luigi.Task):
	
	def requires(self):
		return CollectTestData, CollectTrainData, GetFeatureList

	def output(self):
		return luigi.LocalTarget(preds)
		
	def run(self):
		
		if DEBUG:
			print('Running Random Forest Classifier...')

		runForrest=RandomForestClassifier(n_estimators=(200), n_jobs=4)
		runForrest.fit(trainData[features], trainData['reactivate_7_days'])

		preds=runForrest.predict_proba(testData[features])

		auc=metrics.roc_auc_score(y_score=preds[:,1], y_true=testData['reactivate_7_days'])
			
		logloss=metrics.log_loss(y_pred=preds[:,1], y_true=testData['reactivate_7_days'])

		if DEBUG:
			print(logloss)

			print(auc)

			for x in range(0,len(features)):
				print (features[x] + '\t\t-- ' + str(runForrest.feature_importances_[x]))

class RunGLMPredections(luigi.Task):

	def requires(self):
		return CollectTestData, CollectTrainData, GetFeatureList

	def output(self):
		return luigi.LocalTarget(glm.predict_proba(testData[features]))
		
	def run(self):
		
		if DEBUG:
			print('Building Generalized Linear Model...')
			
		glm=linear_model.LogisticRegression()
		glm.fit(trainData[features], trainData['reactivate_7_days'])
	
class RunGBMPredictions(luigi.Task):

	def requires(self):
		return CollectTestData, CollectTrainData, GetFeatureList

	def output(self):
		return luigi.LocalTarget(runGBM.predict_proba(testData[features]))
		
	def run(self):
	
		if DEBUG:
			print('Building Gradient Boosted Model...')
			
		runGBM=ensemble.GradientBoostingClassifier(learning_rate=0.1, max_features='sqrt', subsample=0.8, random_state=10, max_depth=10, min_samples_leaf=20, min_samples_split=10, n_estimators=30)
		runGBM.fit(trainData[features], trainData['reactivate_7_days'])

class AveragePreds(luigi.Task):

	def requires(self):
		return RunGBMPredictions, RunGLMPredections, RunRandomForestPredictions

	def output(self):
		return luigi.LocalTarget('done')
		
	def run(self):
		 return (testData['ra7_rf_preds'] + testData['ra7_glm_preds'] + testData['ra7_gbm_preds']) / 3
	
class WritePreds(luigi.Task):

	def requires(self):
		return RunGBMPredictions, RunGLMPredections, RunRandomForestPredictions

	def output(self):
		return luigi.LocalTarget('done')
		
	def run(self):

		if DEBUG:
			print('Writing Model Predictions to database...')
			
		dataFrame['churn_n']=dotZeroBugFix(dataFrame['churn_n'])
		
		dataFrame['user_id']=dotZeroBugFix(dataFrame['user_id'])
		
		dataFrame['model_date']='%s-%s-%s' % (TIMES['todaysDate'][6:],TIMES['todaysDate'][:2],TIMES['todaysDate'][3:5])
			
		dataFrame[OUTPUT_COLUMNS].to_csv('churns_with_preds.csv', index = False, sep = ',')

		S3upload('churns_with_preds.csv', "/churn/", "analytics-")	

		executeSQL("COPY .churn_preds FROM 's3://analytics-/churn/churns_with_preds.csv' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ',';")

		executeSQL("INSERT INTO .churn_summary VALUES ('%s', '%s', %s, %s, %s, %s, %s, 0, 0, 0, 0, 0)" % (TIMES['twoWeekAgo'], TIMES['todaysDate'], str(dataFrame.shape[0]), str(int(dataFrame['ra7_glm_preds'].sum())), str(int(dataFrame['ra7_rf_preds'].sum())), str(int(dataFrame['ra7_gbm_preds'].sum())), str(int(dataFrame['ra7_avg_preds'].sum()))))