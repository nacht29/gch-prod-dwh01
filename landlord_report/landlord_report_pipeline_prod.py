from pygcp.utils import *
from pygcp.bigquery import *
from pygcp.gcs_bucket import *
from pygcp.google_drive import *
from datetime import datetime, timezone, timedelta

TIME_ZONE = timezone(timedelta(hours=8))
START_DATE = datetime(2025, 3, 11, tzinfo=TIME_ZONE)

JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/utils/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = JSON_KEYS_PATH
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

SQL_SCRIPTS_PATH = '/home/yanzhe/gch-prod-dwh01/landlord_report/sql-scripts'

SCOPES = ['https://www.googleapis.com/auth/drive']
PARENT_FOLDER_ID = '1-sNl83to2N7-GZQzESTwGj1gGeMT0_n3'
SHARED_DRIVE_ID = '0AJjN4b49gRCrUk9PVA'

SLICE_BY_ROWS = 1000000 - 1

def get_folder_name():
	month_num = get_month(False)
	if month_num < 10:
		month_num = f'0{month_num}'
	month_name = get_month(True)
	year = get_year()
	folder_name = f'{year}{month_num} - {month_name}'
	return folder_name

def landlord_report_pipeline_dev():
	service = build_drive_service(SERVICE_ACCOUNT)
	folder_name = get_folder_name()
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')
	cur_date = (datetime.today() - timedelta(days=1))

	for cur_script in sql_scripts:
		outfile_name = gen_file_name('', cur_script, '.sql', '.csv', cur_date.strftime("%Y%m%d"))
		csv_files = bq_to_csv(
			bq_client=bq_client,
			sql_script=f'{SQL_SCRIPTS_PATH}/{cur_script}',
			slice_row=SLICE_BY_ROWS,
			outfile_name=outfile_name,
			replace_in_query=[("cur_date", f"'{cur_date.strftime("%Y-%m-%d")}'")],
			log=True
		)

		folder_data = drive_autodetect_folders(
			service=service,
			is_shared_drive=True,
			parent_folder_id=PARENT_FOLDER_ID,
			folder_name=folder_name,
			create_folder=True
		)
		folder_id = folder_data['id']

		local_csv_to_gdrive(
			service=service,
			is_shared_drive=True,
			main_drive_id=SHARED_DRIVE_ID,
			dst_folder_id=folder_id,
			csv_files=csv_files,
			update_dup=True,
			log=False
		)

if __name__ == '__main__':
	try:
		landlord_report_pipeline_dev()
	except Exception:
		print("pipeline_error: landlord_report_pipeline_dev failed")
		raise
