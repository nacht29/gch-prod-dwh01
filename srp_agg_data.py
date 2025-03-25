import os
import io
import calendar
import pandas as pd
import logging as log
from datetime import datetime
from google.cloud import bigquery as bq
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

'''
Credentials
'''
# JSON_KEYS_PATH = 'json-keys/gch-prod-dwh01-data-pipeline.json'
JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

# set up Bucket credentials to load CSV to bucket
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SRP_PARENT_FOLDER_ID = '10VMUdmAQXPVZwxjB-LNjIC0bDgslkl0l'

'''
Local files
'''
# PY_LOGS_DIR = '/mnt/c/Users/Asus/Desktop/py_log'
PY_LOGS_DIR = '/home/yanzhe/py_log'
os.makedirs(PY_LOGS_DIR, exist_ok=True)

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

'''
Helper functions
'''
def get_loc_date(file_name):
	names = file_name.replace('.csv', '').split('_')
	return (names[1], names[2])

def snake_case(col: str) -> str:
	return col.lower().strip().replace(' ', '_').replace('-', '_')

'''
Drive functions
'''
def get_drive_file_id(service, parent_folder_id: str, is_folder: bool):
	# mimetype ='application/vnd.google-apps.folder' if is_folder == True
	# else mimetype !='application/vnd.google-apps.folder' 
	query = f"""
	'{parent_folder_id}' in parents
	and mimeType{'=' if is_folder else '!='}'application/vnd.google-apps.folder'
	and trashed=false
	"""

	request = service.files().list(
		q=query,
		fields='files(id, name)',
		supportsAllDrives=True,
		includeItemsFromAllDrives=True
	).execute()
	return request.get('files', [])

# read data from a CSV file in drive
# write the read data into into binary buffer for CSV
# add location and date columns
def read_csv_from_drive(service, file_metadata):
	try:
		request = service.files().get_media(fileId=file_metadata['id'])
		csv_buffer = io.BytesIO()
		downloader = MediaIoBaseDownload(csv_buffer, request)

		while True:
			status, done = downloader.next_chunk()
			log.info(f"{file_metadata['name']} {int(status.progress() * 100)}%")
			if done:
				break

		csv_buffer.seek(0)  # Reset buffer position
		results_df = pd.read_csv(csv_buffer)

		# add location and date
		loc, file_date = get_loc_date(file_metadata['name'])
		results_df['location'] = loc
		results_df['date'] = file_date

		return results_df

	except HttpError as error:
		log.info(f"Error processing {file_metadata['name']}: {error}")
		return pd.DataFrame()

# iterates through each folder, and in each folder each CSV
# union all CSV data into one main_df
def read_files_from_drive():
	creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
	service = build('drive', 'v3', credentials=creds)

	main_df = pd.DataFrame()
	yy_mm_folders = get_drive_file_id(service, SRP_PARENT_FOLDER_ID, True)

	for folder in yy_mm_folders:
		csv_files = get_drive_file_id(service, folder['id'], False)
		for csv_file in csv_files:
			results_df = read_csv_from_drive(service, csv_file)
			main_df = pd.concat([main_df, results_df], ignore_index=True)

	return main_df

def load_table(df, table_suffix: str):
	table_ref = f'gch-prod-dwh01.srp_data.srp_possales_{table_suffix}'
	job_config = bq.LoadJobConfig(write_disposition='WRITE_TRUNCATE', autodetect=True)
	job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
	return job.result()

def load_bq(df):
	for location in df['location'].unique():
		location_df = df[df['location'] == location]
		load_table(location_df, location)

def main():
	main_df = read_files_from_drive()
	
	if not main_df.empty:
		main_df['date'] = pd.to_datetime(main_df['date'].astype(str)).dt.date
		main_df.columns = [snake_case(col) for col in main_df.columns]
		load_bq(main_df)
	else:
		log.info("No data processed")

if __name__ == '__main__':
	main()