import os
import calendar
import pandas as pd
import logging as log
from io import BytesIO
from google.cloud import storage
from google.cloud import bigquery as bq
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from datetime import datetime, timezone, timedelta

'''
DATETIME constants
'''
TIME_ZONE = timezone(timedelta(hours=8))
START_DATE = datetime(2025, 3, 11, tzinfo=TIME_ZONE)

'''
CREDENTIALS	
'''
JSON_KEYS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space workspace/giant/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

# set up credentials for BQ and Drive to query data
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

'''
LOCAL FILE PATHS
'''
SQL_SCRIPTS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space workspace/giant/gch-prod-dwh01/sql-scripts/itemmaster'

OUTFILES_DIR = '/mnt/c/Users/Asus/Desktop/cloud-space workspace/giant/gch-prod-dwh01/src/itemmaster/outfiles'
os.makedirs(OUTFILES_DIR, exist_ok=True)

PY_LOGS_DIR = '/mnt/c/Users/Asus/Desktop/cloud-space workspace/giant/gch-prod-dwh01/src/itemmaster/py_log'
os.makedirs(PY_LOGS_DIR, exist_ok=True)

'''
GOOGLE DRIVE PARAMS
'''
SCOPES = ['https://www.googleapis.com/auth/drive']

PARENT_FOLDER_ID = '1eEYY7DPK01hP7zjPRN4fj-XvCi5hGbDk'
SHARED_DRIVE_ID = '0AJjN4b49gRCrUk9PVA'

'''
OUTPUT FILE CONFIG
'''
SLICE_BY_ROWS = 1000000 - 1

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

log.info(f'{datetime.now()} exapp_pipeline_prod --initiated')

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

def gen_file_name(infile_name:str, infile_type:str, outfile_type:str):
	file_name = f"itemmaster_{infile_name.replace('.sql', '.xlsx')}"
	return file_name

'''
Load Google Drive
'''

# get the name of the department where the file should be stored

def load_gdrive(excel_buffer, out_filename:str):
	creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
	service = build('drive', 'v3', credentials=creds)

	query = f"""
	'{PARENT_FOLDER_ID}' in parents
	and name = '{out_filename}'
	and trashed=false
	"""

	# execute the query
	results = service.files().list(
			q=query,
			fields='files(id, name)',
			supportsAllDrives=True
	).execute()

	# get dup file id
	dup_files = results.get('files')

	# xlsx file metadata
	file_metadata = {
		'name': out_filename,
		'parents': [PARENT_FOLDER_ID],
		'driveId': SHARED_DRIVE_ID
	}

	media = MediaIoBaseUpload(
		excel_buffer,
		mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
		resumable=True
	)

	# update or create files
	if dup_files:
		log.info(f"{datetime.now()} Updating {dup_files[0]['name']}")
		dup_file_id = dup_files[0]['id']
		file = service.files().update(
			fileId=dup_file_id,
			media_body=media,
			supportsAllDrives=True
		).execute()

	else:
		service.files().create(
			body=file_metadata,
			media_body=media,
			fields='id',
			supportsAllDrives=True
		).execute()

def itemmaster():
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')

	for script in sql_scripts:
		try:
			with open(f'{SQL_SCRIPTS_PATH}/{script}', 'r') as cur_script:
				log.info(f'\n\n{datetime.now()} Query: {script}')
				query = ' '.join([line for line in cur_script])
				results_df = bq_client.query(query).to_dataframe()
				log.info(f'Results: {results_df.shape}')

				for cur_row in range(0, len(results_df), SLICE_BY_ROWS):
					# file_ver: 1 -> (0,99), 2 -> (100, 199) etc
					file_ver = cur_row // SLICE_BY_ROWS + 1
					# get subset of full query result (sliced by rows)
					subset = results_df.iloc[cur_row:cur_row + SLICE_BY_ROWS]
					out_filename = gen_file_name(script, '.sql', '.xlsx')
				
					log.info(f'{datetime.now()} creating xlsx binary for {out_filename}')
					excel_buffer = BytesIO()
					with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
						subset.to_excel(writer, index=False, header=True)
					excel_buffer.seek(0)
					datetime.now()
					log.info(f'{datetime.now()} {out_filename} binary created')

					# load to drive
					try:
						log.info(f'{datetime.now()} loading {out_filename} to Drive')
						load_gdrive(excel_buffer, out_filename)
						log.info(f'{datetime.now()} {out_filename} loaded to Drive\n')
					except Exception as error:
						log.error(f'Failed to load to Drive.\n\n{error}')
						raise
		except Exception:
			raise

if __name__ == '__main__':
	itemmaster()
