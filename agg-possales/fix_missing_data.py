import io
import datetime
import pandas as pd
from google.cloud import bigquery as bq
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

'''
Credentials
'''
# JSON_KEYS_PATH = 'json-keys/gch-prod-dwh01-data-pipeline.json'
JSON_KEYS_PATH = '../json-keys/gch-prod-dwh01-data-pipeline.json'
# JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

# set up Bucket credentials to load CSV to bucket
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SRP_PARENT_FOLDER_ID = '10VMUdmAQXPVZwxjB-LNjIC0bDgslkl0l'

def get_loc_date(file_name):
	names = file_name.replace('.csv', '').split('_')
	return (names[1], names[2])

def snake_case(col: str) -> str:
	return col.lower().strip().replace(' ', '_').replace('-', '_')

def process_csv_from_drive(service, file_metadata):
	try:
		request = service.files().get_media(fileId=file_metadata['id'])
		csv_buffer = io.BytesIO()
		downloader = MediaIoBaseDownload(csv_buffer, request)

		while True:
			status, done = downloader.next_chunk()
			print(f"{file_metadata['name']} {int(status.progress() * 100)}%")
			if done:
				break

		csv_buffer.seek(0)  # Reset buffer position
		results_df = pd.read_csv(csv_buffer, thousands=',', decimal='.')

		# add location and date
		loc, file_date = get_loc_date(file_metadata['name'])

		results_df['location'] = loc
		results_df['date'] = file_date

		return results_df
	except Exception as error:
		raise

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


def read_process_files_from_drive():
	creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
	service = build('drive', 'v3', credentials=creds)

	main_df = pd.DataFrame()
	yy_mm_folders = get_drive_file_id(service, SRP_PARENT_FOLDER_ID, True)

	file_proccessed = 0
	for folder in yy_mm_folders:
		if folder['name'].endswith('202408'):
			csv_files = get_drive_file_id(service, folder['id'], False)
			for csv_file in csv_files:
				results_df = process_csv_from_drive(service, csv_file)
				main_df = pd.concat([main_df, results_df], ignore_index=True)
				file_proccessed += 1
				print(f'\n\nfile_processed: {file_proccessed}\n\n')

	return main_df

def load_table(df, table_suffix: str):
	table_ref = f'gch-prod-dwh01.srp_data.srp_possales_{table_suffix}_copy3'
	job_config = bq.LoadJobConfig(
		write_disposition='WRITE_APPEND',
		autodetect=True
	)
	job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
	return job.result()

#===================================================================================#
##################################### MAIN PROGRAM ##################################
#===================================================================================#

print(f'{datetime.datetime.now()} extract expected_df')
expected_df = pd.read_excel('srp.xlsx', sheet_name='Sheet3', header=0)
expected_df = expected_df.rename(columns={
	'total_sales_qty': 'total_qty_sales'
})
print(f'{datetime.datetime.now()} exit expected_df\n\n')

date_loc_missing = [('2024-08-01', 1216), ('2024-08-01', 1221), ('2024-08-01', 1227), ('2024-08-01', 1228), ('2024-08-02', 1216), ('2024-08-02', 1221), ('2024-08-02', 1227),('2024-08-02', 1228), ('2024-08-03', 1216), ('2024-08-03', 1221), ('2024-08-03', 1227), ('2024-08-03', 1228), ('2024-08-04', 1216), ('2024-08-04', 1221), ('2024-08-04', 1227), ('2024-08-04', 1228), ('2024-08-05', 1216), ('2024-08-05', 1221), ('2024-08-05', 1227), ('2024-08-05', 1228), ('2024-08-06', 1216), ('2024-08-06', 1221), ('2024-08-06', 1227), ('2024-08-06', 1228), ('2024-09-01', 1216), ('2024-09-01', 1227), ('2024-09-01', 1228), ('2024-09-02', 1216), ('2024-09-02', 1227), ('2024-09-02', 1228), ('2024-09-03', 1216), ('2024-09-03', 1227), ('2024-09-03', 1228), ('2024-09-04', 1216), ('2024-09-04', 1227), ('2024-09-04', 1228), ('2024-09-05', 1216), ('2024-09-05', 1227), ('2024-09-05', 1228), ('2024-09-06', 1216), ('2024-09-06', 1227), ('2024-09-06', 1228), ('2024-09-07', 1216), ('2024-09-07', 1227), ('2024-09-07', 1228), ('2024-09-08', 1216), ('2024-09-08', 1227), ('2024-09-08', 1228), ('2024-09-09', 1216), ('2024-09-09', 1227), ('2024-09-09', 1228), ('2024-09-10', 1216), ('2024-09-10', 1227), ('2024-09-10', 1228), ('2024-09-11', 1216), ('2024-09-11', 1227), ('2024-09-11', 1228), ('2024-09-12', 1216), ('2024-09-12', 1227), ('2024-09-12', 1228), ('2024-09-13', 1216), ('2024-09-13', 1227), ('2024-09-13', 1228), ('2024-09-14', 1216), ('2024-09-14', 1227), ('2024-09-14', 1228), ('2024-09-15', 1216), ('2024-09-15', 1227), ('2024-09-15', 1228), ('2024-09-16', 1216), ('2024-09-16', 1227), ('2024-09-16', 1228), ('2024-09-17', 1216), ('2024-09-17', 1227), ('2024-09-17', 1228), ('2024-09-18', 1216), ('2024-09-18', 1227), ('2024-09-18', 1228), ('2024-09-19', 1216), ('2024-09-19', 1227), ('2024-09-19', 1228), ('2024-09-20', 1216), ('2024-09-20', 1227), ('2024-09-20', 1228), ('2024-10-01', 1216), ('2024-10-01', 1217), ('2024-10-01', 1227), ('2024-10-01', 1228), ('2024-10-02', 1216), ('2024-10-02', 1217), ('2024-10-02', 1227), ('2024-10-02', 1228), ('2024-10-03', 1216), ('2024-10-03', 1217), ('2024-10-03', 1227), ('2024-10-04', 1216), ('2024-10-04', 1227), ('2024-10-05', 1216), ('2024-10-05', 1227), ('2024-10-06', 1216), ('2024-10-06', 1227), ('2024-10-07', 1216), ('2024-10-07', 1217), ('2024-10-07', 1227), ('2024-10-08', 1216), ('2024-10-08', 1217), ('2024-10-08', 1227), ('2024-10-09', 1216), ('2024-10-09', 1217), ('2024-10-09', 1227), ('2024-10-10', 1216), ('2024-10-10', 1217), ('2024-10-10', 1227), ('2024-10-11', 1216), ('2024-10-11', 1217), ('2024-10-11', 1227), ('2024-10-12', 1216), ('2024-10-12', 1217), ('2024-10-12', 1227), ('2024-10-13', 1216), ('2024-10-13', 1217), ('2024-10-13', 1227), ('2024-10-14', 1216), ('2024-10-14', 1217), ('2024-10-15', 1216), ('2024-10-15', 1217), ('2024-10-15', 1227)]

def main():
	creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
	service = build('drive', 'v3', credentials=creds)

	for mis_date, mis_loc in date_loc_missing:
		date_str = mis_date.replace('-', '')
		folder_name = f'SRP_{date_str[:6]}'
		file_name = f'SRP_{mis_loc}_{date_str}.csv'

		yymm_folder_id = get_drive_file_id(service, SRP_PARENT_FOLDER_ID, True)

		if yymm_folder_id == []:
			print(f'\n\n{folder_name} - {file_name} cannot be found')
			break
		print(yymm_folder_id)

if __name__ == '__main__':
	main()