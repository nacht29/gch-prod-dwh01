from pygcp.utils import *
from pygcp.bigquery import *
from pygcp.gcs_bucket import *
from pygcp.google_drive import *
from datetime import datetime, timezone, timedelta

TIME_ZONE = timezone(timedelta(hours=8))
START_DATE = datetime(2025, 3, 11, tzinfo=TIME_ZONE)

# JSON_KEYS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/utils/json-keys/gch-prod-dwh01-data-pipeline.json'
JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/utils/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = JSON_KEYS_PATH
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

# SQL_SCRIPTS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/landlord_report/sql-scripts'
SQL_SCRIPTS_PATH = '/home/yanzhe/gch-prod-dwh01/landlord_report/sql-scripts'

# PY_LOGS_DIR = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/src/landlord_report/py_log'
PY_LOGS_DIR = '/home/yanzhe/gch-prod-dwh01/src/landlord_report/py_log'
os.makedirs(PY_LOGS_DIR, exist_ok=True)

SCOPES = ['https://www.googleapis.com/auth/drive']
PARENT_FOLDER_ID = '1-sNl83to2N7-GZQzESTwGj1gGeMT0_n3'
SHARED_DRIVE_ID = '0AJjN4b49gRCrUk9PVA'

SLICE_BY_ROWS = 1000000 - 1

def get_folder_name():
	month_num = get_month(False)
	month_name = get_month(True)
	year = get_year()
	folder_name = f'{year}{month_num} - {month_name}'
	return folder_name

def landlord_bq_to_df(bq_client, sql_script:str, log=False, ignore_error=False):
	with open(sql_script, 'r') as cur_script:
		if log:
			print(f'\n\n{datetime.now()} Query: {sql_script}')

		try:
			query = ' '.join([line for line in cur_script])
			date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
			query = query.replace("cur_date", f"'{date}'")
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

def landlord_bq_to_csv(bq_client,
			  sql_script:str,
			  slice_row:int,
			  outfile_name:str,
			  log=False,
			  ignore_error=False
) -> tuple:

	if not 0 < slice_row <= 1000000:
		raise ValueError('Invalid slice length.')

	results_df = landlord_bq_to_df(bq_client, sql_script, log, ignore_error)
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

def landlord_report_pipeline_dev():
	service = build_drive_service(SERVICE_ACCOUNT)
	folder_name = get_folder_name()
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')
	date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

	for cur_script in sql_scripts:
		outfile_name = gen_file_name('', cur_script, '.sql', '.csv', date)
		csv_files = landlord_bq_to_csv(bq_client, f'{SQL_SCRIPTS_PATH}/{cur_script}', SLICE_BY_ROWS, outfile_name, log=True)

		folder_data = drive_autodetect_folders(service, PARENT_FOLDER_ID, folder_name, create_folder=True)
		folder_id = folder_data['id']

		local_csv_to_gdrive(
			service=service,
			main_drive_id=SHARED_DRIVE_ID,
			dst_folder_id=folder_id,
			csv_files=csv_files,
			update_dup=True,
			log=Fals
		)

if __name__ == '__main__':
	try:
		landlord_report_pipeline_dev()
	except Exception:
		print("pipeline_error: landlord_report_pipeline_dev failed")
		raise
