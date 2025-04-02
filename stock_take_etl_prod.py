import os
import calendar
import pandas as pd
import logging as log
from io import BytesIO
from google.cloud import bigquery as bq
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from datetime import date, datetime, timezone, timedelta

'''
CREDENTIALS	
'''
JSON_KEYS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space workspace/giant/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
# JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

# set up credentials for BQ and Drive to query data
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)

'''
LOCAL FILE PATHS
'''
# SQL_SCRIPTS_PATH = '/home/yanzhe/gch-prod-dwh01/sql-scripts/stock_take'
SQL_SCRIPTS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space workspace/giant/gch-prod-dwh01/sql-scripts/stock_take'

# OUTFILES_DIR = '/home/yanzhe/outfiles'
OUTFILES_DIR = '/mnt/c/Users/Asus/Desktop/cloud-space workspace/giant/outfiles'
os.makedirs(OUTFILES_DIR, exist_ok=True)

# PY_LOGS_DIR = '/home/yanzhe/py_log'
PY_LOGS_DIR = '/mnt/c/Users/Asus/Desktop/cloud-space workspace/giant/py_log'
os.makedirs(PY_LOGS_DIR, exist_ok=True)

'''
GOOGLE DRIVE PARAMS
'''
SCOPES = ["https://www.googleapis.com/auth/drive"]

DEST_FOLDER_ID = '1CT3EyIk0PuLRuWHKBA0I0q5BmuH7cHWA' # use this for the actual prod
# POSSALES_RL_FOLDER_ID = '1iQDbpxsqa8zoEIREJANEWau6HEqPe7hF' # GCH Report > Supply Chain (mock drive)

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

'''
Helper functions
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

def gen_file_name(prefix:str, infile_name:str, infile_type:str, outfile_type:str, suffix:str):
	file_name = infile_name.replace(infile_type, '')
	final_file_name = f"{prefix}{file_name}{suffix}{outfile_type}"
	return final_file_name

'''
Query from BQ
'''
def bq_to_df(bq_client, sql_script:str, log=False, ignore_error=False):
	with open(sql_script, 'r') as cur_script:
		if log:
			print(f'\n\n{datetime.now()} Query: {sql_script}')

		try:
			query = ' '.join([line for line in cur_script])
			results_df = bq_client.query(query).to_dataframe()
		except Exception:
			print(f'{sql_script} query failed.')
			if ignore_error:
				results_df = pd.DataFrame() 
				return results_df
			raise

		if log:
			print(f'Results: {results_df.shape}')

	return (results_df)

def bq_to_csv(bq_client, sql_script:str, slice_row:int, outfile_name:str, log=False, ignore_error=False):
	if not 0 < slice_row <= 1000000:
		raise ValueError('Invalid slice length.')

	results_df = bq_to_df(bq_client, sql_script, log, ignore_error)
	csv_buffers = []

	for cur_row in range(0, len(results_df), slice_row):
		file_ver = cur_row // slice_row + 1
		subset_df = results_df.iloc[cur_row:cur_row + slice_row]
		cur_outfile_name = outfile_name.replace('.csv', f'_{file_ver}.csv')

		print(f'{datetime.now()} creating CSV binary for {cur_outfile_name}')

		cur_buffer = BytesIO()
		subset_df.to_csv(cur_buffer, index=False)
		cur_buffer.seek(0)

		csv_buffers.append((cur_outfile_name, cur_buffer))

		print(f'{datetime.now()} {cur_outfile_name} CSV binary created')

	return csv_buffers

'''
Load to Drive
'''

def build_drive_service(service_account_key):
	scopes = ["https://www.googleapis.com/auth/drive"]
	creds = service_account.Credentials.from_service_account_file(service_account_key, scopes=scopes)
	service = build('drive', 'v3', credentials=creds)
	return service

def drive_get_dup_files(service, dst_folder_id:str, out_filename:str):
	query = f"""
	'{dst_folder_id}' in parents
	and name='{out_filename}'
	and trashed=false
	"""

	results = service.files().list(
			q=query,
			fields='files(id, name)',
			supportsAllDrives=True,
			includeItemsFromAllDrives=True
	).execute()

	# get dup file id
	dup_files = results.get('files', [])

	return dup_files

def drive_create_file(service, file_metadata:dict, media, log=False):
	if log:
		print(f"{datetime.now()} Creating {file_metadata['name']}")

	try:
		service.files().create(
			body=file_metadata,
			media_body=media,
			fields='id',
			supportsAllDrives=True
		).execute()
	except Exception:
		if log:
			print(f'Error processing  {file_metadata['name']}')
		raise

def drive_update_file(service, media, dup_files:list, log:bool):
	if log:
		print(f"{datetime.now()} Updating {dup_files[0]['name']}")

	dup_file_id = dup_files[0]['id']
	try:
		service.files().update(
			fileId=dup_file_id,
			media_body=media,
			supportsAllDrives=True
		).execute()
	except Exception:
		if log:
			print(f'Error processing  {dup_files[0]['name']}')
		raise

def drive_autodetect_folders(service, parent_folder_id:str, folder_name:str, create_folder:bool):
	query = f"""
	'{parent_folder_id}' in parents 
	and name='{folder_name}'
	and mimeType='application/vnd.google-apps.folder' 
	and trashed=false
	"""

	# execute the query
	results = service.files().list(
		q=query,
		fields='files(id, name, modifiedTime)',
		orderBy='modifiedTime desc',
		pageSize=1,
		supportsAllDrives=True,
		includeItemsFromAllDrives=True
	).execute()

	folders_in_drive = results.get('files', []) # files_in_drive = results.get('files', [])

	if folders_in_drive:
		return folders_in_drive[0]
	elif not folders_in_drive and create_folder:
		folder_metadata = {
			'name': folder_name,
			'mimeType': 'application/vnd.google-apps.folder',
			'parents': [parent_folder_id]
		}

		folder = service.files().create(
			body=folder_metadata,
			fields='id, name',
			supportsAllDrives=True
			# note that for .create, there is no need to includeItemsFromAllDrives=True
		).execute()

		return folder

	return []

def local_csv_to_gdrive( service, main_drive_id:str, dst_folder_id:str, csv_buffers:list, update_dup=True, log=False):
	for csv_buffer in csv_buffers:
		# define file metadata and media type
		out_filename = csv_buffer[0]
		file_metadata = {
			'name': out_filename,
			'parents': [dst_folder_id],
			'driveId': main_drive_id
		}

		media = MediaIoBaseUpload(
			csv_buffer[1],
			mimetype='text/csv',
			resumable=True
		)

		if update_dup:
			dup_files = drive_get_dup_files(service, dst_folder_id, out_filename)

			# update existing files or create new ones
			if dup_files:
				drive_update_file(service, media, dup_files, log)
			else:
				drive_create_file(service, file_metadata, media)

		else:
			drive_create_file(service, file_metadata, media)

def stock_take_etl_prod():
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')
	service = build_drive_service(SERVICE_ACCOUNT)

	for cur_script in sql_scripts:
		try:
			outfile = gen_file_name('', cur_script, '.sql', '.csv', '')
			csv_buffers = bq_to_csv(bq_client, f'{SQL_SCRIPTS_PATH}/{cur_script}', SLICE_BY_ROWS, outfile_name=outfile)
		except Exception:
			raise

		parts = outfile.split('_')
		folder_name = parts[-2].title()
		folder_data = drive_autodetect_folders(service, parent_folder_id=DEST_FOLDER_ID, folder_name=folder_name, create_folder=True)
		local_csv_to_gdrive(service, SHARED_DRIVE_ID, folder_data['id'], csv_buffers, update_dup=True)

if __name__ == '__main__':
	stock_take_etl_prod()

'''
	results = service.files().list(
		q=f"'{DEST_FOLDER_ID}' in parents",
		fields="files(id, name)",
		supportsAllDrives=True,
		includeItemsFromAllDrives=True
	).execute()

	print(results)
'''