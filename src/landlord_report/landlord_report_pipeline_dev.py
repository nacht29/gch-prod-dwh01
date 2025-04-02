from pygcp.utils import *
from pygcp.bigquery import *
from pygcp.gcs_bucket import *
from pygcp.google_drive import *
from datetime import datetime, timezone, timedelta

TIME_ZONE = timezone(timedelta(hours=8))
START_DATE = datetime(2025, 3, 11, tzinfo=TIME_ZONE)

JSON_KEYS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
# JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = JSON_KEYS_PATH
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

SQL_SCRIPTS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/sql-scripts/landlord_report'
# SQL_SCRIPTS_PATH = '/home/yanzhe/gch-prod-dwh01/sql-scripts/landlord'

PY_LOGS_DIR = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/src/landlord_report/py_log'
# PY_LOGS_DIR = '/home/yanzhe/gch-prod-dwh01/src/landlord_report/py_log'
os.makedirs(PY_LOGS_DIR, exist_ok=True)

SCOPES = ['https://www.googleapis.com/auth/drive']
PARENT_FOLDER_ID = '1-sNl83to2N7-GZQzESTwGj1gGeMT0_n3'
SHARED_DRIVE_ID = '0AJjN4b49gRCrUk9PVA'

SLICE_BY_ROWS = 1000000 - 1

def landlord_report_pipeline_dev():
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')
	print(sql_scripts)
	service = build_drive_service(SERVICE_ACCOUNT)

	for cur_script in sql_scripts:
		outfile_name = gen_file_name('', cur_script, '.sql', '.csv', '')
		csv_files = bq_to_csv(bq_client, f'{SQL_SCRIPTS_PATH}/{cur_script}', SLICE_BY_ROWS, outfile_name, log=True)

		local_csv_to_gdrive(
			service=service,
			main_drive_id=SHARED_DRIVE_ID,
			dst_folder_id=PARENT_FOLDER_ID,
			csv_files=csv_files,
			update_dup=True,
			log=False
		)

if __name__ == '__main__':
	landlord_report_pipeline_dev()