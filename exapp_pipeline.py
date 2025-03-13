import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.log.logging_mixin import LoggingMixin

import os
import warnings
import subprocess
import calendar
import pandas as pd
from datetime import date, datetime
from google.cloud import bigquery as bq
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.api_core.exceptions import Forbidden, NotFound

# Drive folders
# id_number: (name, folder_id)
# FINAL_FOLDERS = {
# 	1: ('Grocery', '1H87f3BjLv2ddua7Xwiry1t4tXpx6MIt1'),
# 	2: ('Fresh', '1H87f3BjLv2ddua7Xwiry1t4tXpx6MIt1'),
# 	3: 'Preishables',
# 	4: 'Non Foods',
# 	5: 'Health & Beauty',
# 	6: 'GMS'
# }

TIME_ZONE = pendulum.timezone('Asia/Singapore')
START_DATE = datetime(2025, 3, 11, tzinfo=TIME_ZONE)

JSON_KEYS_PATH = 'json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

# Google Drive params
SCOPES = ['https://www.googleapis.com/auth/drive']
PARENT_FOLDER_ID = '1GJ2pAchszF5MWFIV7vNoMyv76xa3Wvbm' # Possales_RL > Hyper

SQL_SCRIPTS_PATH = 'sql-scripts/sc-possalesrl/'

# 1000000
SLICE_BY_ROWS = 100

# set up BQ credentials to query data
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

def file_type_in_dir(file_dir:str, file_type:str):
	if file_dir is None:
		files_in_dir = os.listdir()
	else:
		files_in_dir = os.listdir(file_dir)

	if file_type is None:
		return files_in_dir
	else:
		return [file for file in files_in_dir if file.endswith(file_type)]

def gen_file_name(infile_name:str, infile_type:str, outfile_type:str, ver:int):
	file_name = f'{infile_name.replace(infile_type,'')}_{date.today()}_{ver}.{outfile_type}'
	return file_name

# return (mm,yyyy)
def gen_date() -> tuple:
	month = calendar.month_name[datetime.now().month]
	year = datetime.now().year

	return (month, year)

def query_data():
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')

	for script in sql_scripts:
		with open(f'{SQL_SCRIPTS_PATH}{script}', 'r') as cur_script:
			query = ' '.join([line for line in cur_script])
			results_df = bq_client.query(query).to_dataframe()

			# for cur_row in range(0, len(results_df), SLICE_BY_ROWS):
			for cur_row in range(0, 1000, SLICE_BY_ROWS):
				# file_ver: 1 -> (0,99), 2 -> (100, 199) etc
				file_ver = cur_row // SLICE_BY_ROWS + 1
				# get subset of full query result (sliced by rows)
				subset = results_df.iloc[cur_row:cur_row + SLICE_BY_ROWS]
				out_filename = gen_file_name(script, '.sql', '.csv', file_ver)
				# upload subset as csv
				subset.to_csv(f'{out_filename}', sep='|', encoding='utf-8', index=False, header=True)

def gen_bucket_path():
	month, year = gen_date()
	return f'supply_chain/possales_rl/{year}/{month}'

def load_bucket():
	bucket = bucket_client.get_bucket('gch_extract_drive_01')

	load_files = file_type_in_dir(None, '.csv')
	for file in load_files:
		path_in_bucket = f'{gen_bucket_path()}/{file}'
		# bucket.blob(path_in_bucket).upload()
		blob = bucket.blob(path_in_bucket)
		blob.upload_from_filename(file)

def load_gdrive():
	pass

def remove_outfiles():
	outfiles = file_type_in_dir(None, '.csv')

	for outfile in outfiles:
		os.remove(outfile)

query_data()
load_bucket()
remove_outfiles()