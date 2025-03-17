import os
import io
import logging as log
import calendar
import pandas as pd
from datetime import date, datetime, timezone, timedelta
from google.cloud import bigquery as bq
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

'''
CREDENTIALS	
'''
JSON_KEYS_PATH = 'json-keys/gch-prod-dwh01-data-pipeline.json'
# JSON_KEYS_PATH = '/home/yanzhe/gchexapp01p/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

# set up Bucket credentials to load CSV to bucket
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

'''
GOOGLE DRIVE PARAMS
'''
SCOPES = ['https://www.googleapis.com/auth/drive']
SRP_PARENT_FOLDER_ID = '10VMUdmAQXPVZwxjB-LNjIC0bDgslkl0l'

'''
Local files
'''
CSV_DIR = '/mnt/c/Users/Asus/Desktop/gch-csv'
os.makedirs(CSV_DIR, exist_ok=True)

'''
Get files from Drive
'''
def get_folder_items(service, parent_folder_id:str, is_folder:bool):
	if is_folder == True:
		query = f"""
		'{parent_folder_id}' in parents
		and mimeType='application/vnd.google-apps.folder'
		and trashed=false
		"""
	else:
		query = f"""
		'{parent_folder_id}' in parents
		and mimeType!='application/vnd.google-apps.folder'
		and trashed=false
		"""

	request = service.files().list(
		q=query, 
		fields='files(id, name)',
		supportsAllDrives=True,			# required for shared drives
		includeItemsFromAllDrives=True	# required for shared drives
	).execute()

	folder_items = request.get('files')
	return folder_items

def download_files(service, file_metadata):
	try:
		request = service.files().get_media(fileId=file_metadata['id'])
		file = io.BytesIO()
		downloader = MediaIoBaseDownload(file, request)
		done = False
		while done is False:
			status, done = downloader.next_chunk()
			print(f"{file_metadata['name']} {int(status.progress() * 100)}%")

		download_folder = f'{CSV_DIR}/{file_metadata['name']}'
		with open(download_folder, 'wb') as f:
			f.write(file.getvalue())
			pass

	except HttpError as error:
		status, done = downloader.next_chunk()
		file = None

def get_files_from_drive():
	creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
	service = build('drive', 'v3', credentials=creds)

	main_df = pd.DataFrame()
	yy_mm_folders_metadata = get_folder_items(service, SRP_PARENT_FOLDER_ID, True)
	for yy_mm_md in yy_mm_folders_metadata:
		yy_mm_id = yy_mm_md['id']
		csv_files_metadata = get_folder_items(service, yy_mm_id, False)

		for csv_file_md in csv_files_metadata:
			download_files(service, csv_file_md)

'''
Get CSV files from folder
'''
def file_type_in_dir(file_dir:str, file_type:str):
	if file_dir is None:
		files_in_dir = os.listdir()
	else:
		files_in_dir = os.listdir(file_dir)

	if file_type is None:
		return files_in_dir
	else:
		return [file for file in files_in_dir if file.endswith(file_type)]

def get_loc_date(file_name):
	names = file_name.replace('.csv', '').split('_')
	loc = names[1]
	file_date = names[2]
	return (loc, str(file_date))

'''
Union all CSV as one table
'''
def union_files(main_df):
	csv_files = file_type_in_dir(CSV_DIR, '.csv')

	for csv_file in csv_files:
		cur_filepath = f'{CSV_DIR}/{csv_file}'
		cur_df = pd.read_csv(cur_filepath, sep=',')
		loc, file_date = get_loc_date(csv_file)
		# if (file_date[0:4] == '2023'):
		cur_df['location'] = loc
		cur_df['date'] = file_date
		main_df = pd.concat([main_df, cur_df], ignore_index=True, sort=False)

	return main_df

'''
Load data to BQ
'''
def load_table(df) -> tuple:
	temp_table = f'gch-prod-dwh01.srp.srp_mod_tmp'

	job_config = bq.LoadJobConfig(
		write_disposition='WRITE_TRUNCATE',
		autodetect=True
	)

	job = bq_client.load_table_from_dataframe(
		df,
		temp_table,
		job_config=job_config
	)

	return (job.result(), temp_table)

def main():
	# get_files_from_drive()
	main_df = pd.DataFrame()
	main_df = union_files(main_df)
	main_df['date'] = main_df['date'].astype(str)
	main_df['date'] = pd.to_datetime(main_df['date']).dt.floor('D')
	load_table(main_df)

if __name__ == '__main__':
	main()