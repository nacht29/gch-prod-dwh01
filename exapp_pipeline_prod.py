import os
import calendar
import pyxlsb
import logging as log
import pandas as pd
from datetime import date, datetime, timezone, timedelta
from google.cloud import bigquery as bq
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.api_core.exceptions import Forbidden, NotFound

'''
DATETIME constants
'''
TIME_ZONE = timezone(timedelta(hours=8))
START_DATE = datetime(2025, 3, 11, tzinfo=TIME_ZONE)

'''
CREDENTIALS	
'''
# JSON_KEYS_PATH = 'json-keys/gch-prod-dwh01-data-pipeline.json'
JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

'''
LOCAL FILE PATHS
'''
# SQL_SCRIPTS_PATH = 'sql-scripts/sc-possalesrl'
SQL_SCRIPTS_PATH = '/home/yanzhe/gch-prod-dwh01/sql-scripts/sc-possalesrl'

# OUTFILES_DIR = '/mnt/c/Users/Asus/Desktop/outfiles'
OUTFILES_DIR = '/home/yanzhe/outfiles'
os.makedirs(OUTFILES_DIR, exist_ok=True)

# PY_LOGS_DIR = '/mnt/c/Users/Asus/Desktop/py_log'
PY_LOGS_DIR = '/home/yanzhe/py_log'
os.makedirs(PY_LOGS_DIR, exist_ok=True)

'''
GOOGLE DRIVE PARAMS
'''
SCOPES = ['https://www.googleapis.com/auth/drive']

POSSALES_RL_FOLDER_ID = '1LYITa9mHJZXQyC21_75Ip8_oMwBanfcF' # use this for the actual prod
# POSSALES_RL_FOLDER_ID = '1iQDbpxsqa8zoEIREJANEWau6HEqPe7hF' # GCH Report > Supply Chain (mock drive)

'''
OUTPUT FILE CONFIG
'''
SLICE_BY_ROWS = 500000 - 1

DEPARTMENTS = {
	'1': '1 - GROCERY',
	'2': '2 - FRESH',
	'3': '3 - PERISHABLES',
	'4': '4 - NON FOODS',
	'5': '5 - HEALTH & BEAUTY',
	'6': '6 - GMS'
}

DELIMITER = ','

'''
Logging
'''
month = calendar.month_name[datetime.now().month]
year = datetime.now().year

# create log dir for current month/year
LOG_DIR = f'{PY_LOGS_DIR}/{year}/{month}'
os.makedirs(LOG_DIR, exist_ok=True)

# create log file name with timestamp
log_file_name = f'{datetime.now().strftime("%Y%m%d_%H%M%S")}_pylog.txt'
log_file_fullpath = f'{LOG_DIR}/{log_file_name}'

# config logging
log.basicConfig(
    filename=log_file_fullpath,
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# log to console for debugging
console_handler = log.StreamHandler()
console_handler.setLevel(log.INFO)
log.getLogger().addHandler(console_handler)

log.info('exapp_pipeline_prod --initiated')

# set up credentials for BQ and Drive to query data
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

def main():
	try:
		log.info('Query data from BigQuery: running')
		query_data()
		log.info('Query data from BigQuery: success')
	except Exception as error:
		log.error(f'Query data from BigQuery: failed\n\n{error}')
		export_logs()
		remove_outfiles()
		log.info('Pipeline failed at query_data')
		raise

	try:
		log.info('Load data to GCS Bucket: running')
		load_bucket()
		log.info('Load data to GCS Bucket: success')
	except Exception as error:
		log.error(f'Load data to GCS Bucket: failed\n\n{error}')
		export_logs()
		remove_outfiles()
		log.info('Pipeline failed at load_bucket')
		raise

	try:
		log.info('Load data to Drive: running')
		load_gdrive()
		log.info('Load data to Drive: success')
	except Exception as error:
		log.error(f'Load data to Drive: failed\n\n{error}')
		export_logs()
		remove_outfiles()
		log.info('Pipeline failed at load_gdrive')
		raise

	export_logs()
	remove_outfiles()
	log.info('Pipeline success')

'''
Helper functions
'''

# get the names of specific file types in a dir
# if dir = None			: get from root
# if file_type = None	: get all file names
def file_type_in_dir(file_dir:str, file_type:str):
	if file_dir is None:
		files_in_dir = os.listdir()
	else:
		files_in_dir = os.listdir(file_dir)

	if file_type is None:
		return files_in_dir
	else:
		return [file for file in files_in_dir if file.endswith(file_type)]

# get current month and year
# return (mm,yyyy)
def get_month_year() -> tuple:
	month = calendar.month_name[datetime.now().month]
	year = datetime.now().year

	return (month, year)

# generate file name based on naming concentions
# infile:	possales_rl_{dept}.sql
# outfile:	possales_rl_{dept}_{date}_{ver}_{outfile_type}
# e.g. possales_rl_q.sql -> possales_rl_1_2025-03-16_2.xlsx
def gen_file_name(infile_name:str, infile_type:str, outfile_type:str, ver:int):
	file_name = f"{infile_name.replace(infile_type,'')}_{date.today()}_{ver}.{outfile_type}"
	return file_name

'''
Main processes
'''

# get data from BQ and export as CSV to outfiles/
# slices data by million rows
def query_data():
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')

	# run each script
	for script in sql_scripts:
		with open(f'{SQL_SCRIPTS_PATH}/{script}', 'r') as cur_script:
			query = ' '.join([line for line in cur_script])
			results_df = bq_client.query(query).to_dataframe()

			log.info(f'SQL script: {script}')
			log.info(f'Results: {results_df.shape}')

			# slice the results of eac script
			for cur_row in range(0, len(results_df), SLICE_BY_ROWS):
				# file_ver: 1 -> (0,99), 2 -> (100, 199) etc
				file_ver = cur_row // SLICE_BY_ROWS + 1
				# get subset of full query result (sliced by rows)
				subset = results_df.iloc[cur_row:cur_row + SLICE_BY_ROWS]
				out_filename = gen_file_name(script, '.sql', '.xlsx', file_ver)
				log.info(f'Downloading: {out_filename}')
				# upload subset as excel
				subset.to_excel(f'{OUTFILES_DIR}/{out_filename}', index=False, header=True)
				# subset.to_csv(f'{OUTFILES_DIR}/{out_filename}', sep=DELIMITER, encoding='utf-8', index=False, header=True)

# generate path to GCS Bucket for the file
# detects Year > Month > Dept
def filepath_in_bucket(file_name:str):
	month, year = get_month_year()
	all_dept = DEPARTMENTS
	# possales_r1_10_2025-03-11.xlsx -> 10_2025-03-11.xlsx -> 10
	dept_id = file_name.replace('possales_rl_', '').split('_')[0]
	dept_name = all_dept[dept_id]
	return f'supply_chain/possales_rl/{year}/{month}/{dept_name}/{file_name}'

# load xlsx files to bucket
def load_bucket():
	bucket = bucket_client.get_bucket('gch_extract_drive_01')

	csv_files = file_type_in_dir(OUTFILES_DIR, '.xlsx')

	for xlsx_file in csv_files:
		path_in_bucket = f'{filepath_in_bucket(xlsx_file)}'
		# bucket.blob(path_in_bucket).upload()
		blob = bucket.blob(path_in_bucket)
		blob.upload_from_filename(f'{OUTFILES_DIR}/{xlsx_file}')

# detect and dynamically create folders for file in Drive
def drive_autodetect_folders(service, parent_folder_id:str, folder_name:str):
	'''
	# searches if folder exists in drive
	# returns a list of dict
	# id = file/folder id
	# name = file/folder name
	files = [
		{'id':0, 'name':'A'},
		{'id':1, 'name':'B'}
	]
	'''

	# get all folder names in dest
	# parent_folder_id: folder id folder right before the dest folder
	query = f"""
	'{parent_folder_id}' in parents 
	and name='{folder_name}'
	and mimeType='application/vnd.google-apps.folder' 
	and trashed=false
	"""

	# execute the query
	results = service.files().list(
		q=query,
		fields='files(id,name)',
		supportsAllDrives=True, # required when accessing shared drives
		includeItemsFromAllDrives=True # ONLY NEEDED FOR '''.list()''' method!!!
	).execute()
	files_in_drive = results.get('files') # files_in_drive = results.get('files', [])

	# return dest folder id if detected
	# create dest folder if not yet created
	# this is the dynamic folder creation
	if files_in_drive:
		return files_in_drive[0]['id']
	else:
		file_metadata = {
			'name': folder_name,
			'mimeType': 'application/vnd.google-apps.folder',
			'parents': [parent_folder_id]
		}

		folder = service.files().create(
			body=file_metadata,
			fields='id',
			supportsAllDrives=True # required when accessing shared drives
			# note that for .create, there is no need to includeItemsFromAllDrives=True
		).execute()

		return folder['id']

# get the name of the department where the file should be stored
def get_file_dept(file_name:str) -> str:
	dept = DEPARTMENTS

	# get the number after possales - department id
	file_name = file_name.replace('possales_rl_', '').split('_')[0]
	# return corresponding department name accoridng to department number
	return dept[file_name[0]]

# load xlsx file to Drive
def load_gdrive():
	# authenticate
	creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
	service = build('drive', 'v3', credentials=creds)
	month, year = get_month_year()

	# auto detect folders - create folder if destination folder does not exists
	year_folder_id = drive_autodetect_folders(service, POSSALES_RL_FOLDER_ID, year)
	month_folder_id = drive_autodetect_folders(service, year_folder_id, month)

	# get name of all xlsx files to be loaded
	csv_files = file_type_in_dir(OUTFILES_DIR, '.xlsx')

	# process each xlsx file
	for xlsx_file in csv_files:
		# detect if dept folder for current xlsx file exists
		# if not, create the folder
		# return dept folder id
		dept = get_file_dept(xlsx_file)
		dept_folder_id = drive_autodetect_folders(service, month_folder_id, dept)

		# get all files in dept folder
		query = f"""
		'{dept_folder_id}' in parents
		and name = '{xlsx_file}'
		and trashed=false
		"""

		# execute the query
		results = service.files().list(
				q=query,
				fields='files(id, name)',
				supportsAllDrives=True,
				includeItemsFromAllDrives=True
		).execute()
	
		dup_files = results.get('files')
		log.info(f'dup_files {dup_files}')

		# remove duplicates and write the latest file to Drive
		if dup_files:
			log.info(f"Updating {dup_files[0]['name']}")
			dup_file_id = dup_files[0]['id']
			file = service.files().update(
				fileId=dup_file_id,
				media_body=f'{OUTFILES_DIR}/{xlsx_file}',
				supportsAllDrives=True
			).execute()

		else:
			file_metadata = {
				'name': xlsx_file,
				'parents': [dept_folder_id]
			}

			file = service.files().create(
				body=file_metadata,
				media_body=f'{OUTFILES_DIR}/{xlsx_file}',
				supportsAllDrives=True
			).execute()

'''
Outfiles and logs
'''

# remove all xlsx files from local
def remove_outfiles():
	log.info('Removing outfiles...')
	try:
		
		csv_files = file_type_in_dir(OUTFILES_DIR, '.xlsx')
		for xlsx_file in csv_files:
			os.remove(f'{OUTFILES_DIR}/{xlsx_file}')
	except Exception:
		raise
	
def export_logs():
	bucket = bucket_client.get_bucket('gch_extract_drive_01')
	month, year = get_month_year()
	dir_path_in_bucket = f'supply_chain/possales_rl/airflow-logs/{year}/{month}'

	log.info('Exporting logs...')
	log_files = file_type_in_dir(LOG_DIR, '.txt')
	for log_file in log_files:
		path_in_bucket = f'{dir_path_in_bucket}/{log_file}'
		blob = bucket.blob(path_in_bucket)
		blob.upload_from_filename(f'{LOG_DIR}/{log_file}')

if __name__ == '__main__':
	main()
