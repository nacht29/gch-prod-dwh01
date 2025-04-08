from io import BytesIO
from pygcp.utils import *
from pygcp.bigquery import *
from datetime import datetime, timezone, timedelta

TIME_ZONE = timezone(timedelta(hours=8))
START_DATE = datetime(2025, 3, 11, tzinfo=TIME_ZONE)

# JSON_KEYS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/utils/json-keys/gch-prod-dwh01-data-pipeline.json'
JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/utils/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = JSON_KEYS_PATH
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)

# SQL_SCRIPTS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/landlord_report/sql-scripts'
SQL_SCRIPTS_PATH = '/home/yanzhe/gch-prod-dwh01/sftp/sql-scripts'

SLICE_BY_ROWS = 1000000 - 1

def main():
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')

	txt_buffer = []
	for sql_script in sql_scripts:
		outfile_name = gen_file_name('', sql_script, '.sql', '.csv', '')
		csv_files = bq_to_csv(bq_client, f'{SQL_SCRIPTS_PATH}/{sql_script}', SLICE_BY_ROWS, outfile_name, [], True, False)
		for csv_name, csv_buffer in csv_files:
			csv_buffer.seek(0)
			csv_data = csv_buffer.read()
			txt_buffer = BytesIO(csv_data)
			txt_buffer.append((csv_name.replace('.csv', '.txt'), txt_buffer))

		'''
		i = 0
		for file_name, csv_file in enumerate(csv_files):
			i += 1
			if i >= 2:
				break
			csv_buffer.seek(0)
			txt_filename = file_name.replace('.csv', '.txt')
			with open(txt_filename, 'wb') as txt_file:
				txt_file.write(csv_buffer.read())
		'''