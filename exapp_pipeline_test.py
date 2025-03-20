import os
import calendar
import logging as log
import pandas as pd
from io import BytesIO, MediaIoBaseUpload
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

# set up credentials for BQ and Drive to query data
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

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

SHARED_DRIVE_ID = '0AJjN4b49gRCrUk9PVA'

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
	file_name = f"{infile_name.replace(infile_type,'')}_{date.today()}_{ver}{outfile_type}"
	return file_name

'''
Load Bucket
'''
# generate path to GCS Bucket for the file
# detects Year > Month > Dept
def filepath_in_bucket(file_name:str):
	month, year = get_month_year()
	all_dept = DEPARTMENTS
	# possales_r1_10_2025-03-11.xlsx -> 10_2025-03-11.xlsx -> 10
	dept_id = file_name.replace('possales_rl_', '').split('_')[0]
	dept_name = all_dept[dept_id]
	return f'supply_chain/possales_rl/{year}/{month}/{dept_name}/{file_name}'

# write excel binary to bucket
def load_bucket(excel_buffer, out_filename:str):
	path_in_bucket = filepath_in_bucket(out_filename)
	bucket = bucket_client.get_bucket('gch_extract_drive_01')
	blob = bucket.blob(path_in_bucket)
	blob.upload_from_file(
		excel_buffer,
		content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
	)

'''
Load Google Drive
'''

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


def load_gdrive(excel_buffer, out_filename:str):
	creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
	service = build('drive', 'v3', credentials=creds)

	year_folder_id = drive_autodetect_folders(service, POSSALES_RL_FOLDER_ID, year)
	month_folder_id = drive_autodetect_folders(service, year_folder_id, month)
	dept = get_file_dept(out_filename)
	dept_folder_id = drive_autodetect_folders(service, month_folder_id, dept)

	query = f"""
	'{dept_folder_id}' in parents
	and name = '{out_filename}'
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
	
	file_metadata = {
		'name': out_filename,
		'parents': POSSALES_RL_FOLDER_ID,
		'driveId': SHARED_DRIVE_ID
	}

	media = MediaIoBaseUpload(
		excel_buffer,
		mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
		resumable=True
	)

	# Upload to Google Drive with shared drive support
	service.files().create(
		body=file_metadata,
		media_body=media,
		fields='id',
		supportsAllDrives=True,
		includeItemsFromAllDrives=True
	).execute()


'''
Main process (pipeline)
'''
def exapp_pipeline_test():
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')

	for script in sql_scripts:
		try:
			'''
			Querying
			'''
			with open(f'{SQL_SCRIPTS_PATH}/{script}', 'r') as cur_script:
				log.info(f'{datetime.now()} Query: {script}')
				query = ' '.join([line for line in cur_script])
				results_df = bq_client.query(query).to_dataframe()
				log.info(f'Results: {results_df.shape}')

				'''
				Slicing
				'''	

				# slice the results of eac script
				for cur_row in range(0, len(results_df), SLICE_BY_ROWS):
					# file_ver: 1 -> (0,99), 2 -> (100, 199) etc
					file_ver = cur_row // SLICE_BY_ROWS + 1
					# get subset of full query result (sliced by rows)
					subset = results_df.iloc[cur_row:cur_row + SLICE_BY_ROWS]
					out_filename = gen_file_name(script, '.sql', '.xlsx', file_ver)

					'''
					Write to Excel
					'''
					# init binary buffer for xlsx file
					log.info(f'{datetime.now()} creating xlsx binary for {out_filename}')
					excel_buffer = BytesIO()
					with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
						subset.to_excel(writer, index=False, header=True)
					excel_buffer.seek(0)
					datetime.now()
					log.info(f'{datetime.now()} {out_filename} binary created')

					'''
					Loading
					'''
					# load to BQ
					try:
						log.info(f'{datetime.now()} loading {out_filename} to Bucket')
						load_bucket(excel_buffer, out_filename)
						log.info(f'{datetime.now()} {out_filename} loaded')
					except Exception as error:
						log.error(f'Failed to load to Bucket.\n\n{error}')
						raise
					
					# load to drive
					try:
						log.info(f'{datetime.now()} loading {out_filename} to Drive')
						load_gdrive(excel_buffer, out_filename)
						log.info(f'{datetime.now()} {out_filename} loaded')
					except Exception as error:
						log.error(f'Failed to load to Drive.\n\n{error}')
						raise

					# close the buffer
					excel_buffer.close()
					
		except Exception as error:
			raise

exapp_pipeline_test()
