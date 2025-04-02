import io
import datetime
import pandas as pd
from google.cloud import bigquery as bq
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

pd.set_option('display.max_columns', None)

'''
Credentials
'''
# JSON_KEYS_PATH = 'json-keys/gch-prod-dwh01-data-pipeline.json'
JSON_KEYS_PATH = '../json-keys/gch-prod-dwh01-data-pipeline.json'
# JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = JSON_KEYS_PATH

# set up Bucket credentials to load CSV to bucket
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)

SCOPES = ['https://www.googleapis.com/auth/drive']
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

		csv_buffer.seek(0)
		results_df = pd.read_csv(csv_buffer, thousands=',', decimal='.')
		results_df.columns = [snake_case(col) for col in results_df.columns]

		# add location and date
		loc, file_date = get_loc_date(file_metadata['name'])

		results_df['location'] = loc
		results_df['date'] = pd.to_datetime(file_date).date()
		print(results_df.head(2))
		return results_df

	except Exception as error:
		raise

def drive_get_fileID_by_name(service, parent_folder_id: str, file_name: str):
	query = f"""
	'{parent_folder_id}' in parents
	and name = '{file_name}'
	and trashed=false
	"""
	
	try:
		response = service.files().list(
			q=query,
			fields='files(id, name, modifiedTime)',
			orderBy='modifiedTime desc',
			pageSize=1,
			supportsAllDrives=True,
			includeItemsFromAllDrives=True,
		).execute()
		
		files = response.get('files', [])
		if files:
			return files[0]
		else:
			return None
			
	except Exception:
		return None

def drive_get_folderID_by_name(service, parent_folder_id: str, folder_name: str):
	query = f"""
	'{parent_folder_id}' in parents
	and name = '{folder_name}'
	and mimeType = 'application/vnd.google-apps.folder'
	and trashed=false
	"""
	
	try:
		response = service.files().list(
			q=query,
			fields='files(id, name)',
			supportsAllDrives=True,
			includeItemsFromAllDrives=True,
		).execute()
		
		folders = response.get('files', [])
		return folders[0] if folders else None
		
	except Exception:
		return None

def load_to_bq(df, table_suffix: str):
	table_ref = f'gch-prod-dwh01.srp_data.srp_possales_{table_suffix}'
	
	# get existing table in schema
	table = bq_client.get_table(table_ref)
	for field in table.schema:
		print(f"{field.name}: {field.field_type}")
	existing_schema = table.schema
	
	# get existing columns from table
	existing_columns = [field.name for field in existing_schema]
	
	# filter the DataFrame to only include columns that exist in the table
	df = df[[col for col in df.columns if col in existing_columns]]
	
	# convert date column to datetime if it exists
	if 'date' in df.columns:
		df['date'] = pd.to_datetime(df['date']).dt.date

	
	job_config = bq.LoadJobConfig(
		write_disposition='WRITE_APPEND',
		autodetect=False # disable autodetect since we're using existing schema
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
# date_loc_missing = [('2024-10-14', 1216), ('2024-10-14', 1217), ('2024-10-14', 1221), ('2024-10-14', 1226), ('2024-10-14', 1227), ('2024-10-14', 1228)]

after_process = []
missing_csv = []

def main():
	creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
	service = build('drive', 'v3', credentials=creds)

	for mis_date, mis_loc in date_loc_missing:
		date_str = mis_date.replace('-', '')
		folder_name = f'SRP_{date_str[:6]}'
		file_name = f'SRP_{mis_loc}_{date_str}.csv'

		print(f"\nProcessing {file_name}...")

		# get yyyy_mm folder metadata
		yymm_folder_metadata = drive_get_folderID_by_name(service, SRP_PARENT_FOLDER_ID, folder_name)
		if not yymm_folder_metadata:
			print(f"⚠️ Folder {folder_name} not found")
			after_process.append((mis_date, mis_loc, f'Folder {folder_name} not found'))
			continue

		# get CSV files
		csv_file_metadata = drive_get_fileID_by_name(service, yymm_folder_metadata['id'], file_name)
		if not csv_file_metadata:
			print(f"⚠️ File {file_name} not found in {folder_name}")
			after_process.append((mis_date, mis_loc, f'{file_name} not found'))
			continue

		try:
			# Process the file
			print(f"Found file: {csv_file_metadata['name']} (ID: {csv_file_metadata['id']})")
			results_df = process_csv_from_drive(service, csv_file_metadata)

			# Delete duplicates
			delete_dup_query = f"""
			DELETE FROM `gch-prod-dwh01.srp_data.srp_possales_{mis_loc}_copy2`
			WHERE date = '{mis_date}'
			"""
			bq_client.query(delete_dup_query).result()

			# Load to BigQuery
			load_to_bq(results_df, f'{mis_loc}_copy2')

			# Verify the data was loaded
			verify_query = f"""
			SELECT SUM(total_qty_sales) AS total 
			FROM `gch-prod-dwh01.srp_data.srp_possales_{mis_loc}_copy2`
			WHERE date = '{mis_date}'
			"""
			verify_df = bq_client.query(verify_query).to_dataframe()
			
			if verify_df.empty or verify_df['total'].iloc[0] is None:
				print(f"⚠️ Data still missing for {mis_date} {mis_loc}")
				after_process.append((mis_date, mis_loc, "Verification failed"))
			else:
				print(f"✅ Successfully processed {mis_date} {mis_loc}")
				after_process.append((mis_date, mis_loc, "Success"))

		except Exception as e:
			print(f"❌ Error processing {mis_date} {mis_loc}: {str(e)}")
			after_process.append((mis_date, mis_loc, f"Error: {str(e)}"))

if __name__ == '__main__':
	main()
