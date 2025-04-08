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

def bq_to_txt(bq_client,
			  sql_script:str,
			  slice_row:int,
			  outfile_name:str,
			  replace_in_query:list=[],
			  log=False,
			  ignore_error=False
) -> tuple:

	if not 0 < slice_row <= 1000000:
		raise ValueError('Invalid slice length.')

	results_df = bq_to_df(bq_client, sql_script, replace_in_query, log, ignore_error)
	txt_buffers = []

	for cur_row in range(0, len(results_df), slice_row):
		file_ver = cur_row // slice_row + 1
		subset_df = results_df.iloc[cur_row:cur_row + slice_row]
		cur_outfile_name = outfile_name.replace('.txt', f'_{file_ver}.txt')

		print(f'{datetime.now()} creating txt binary for {cur_outfile_name}')

		cur_buffer = BytesIO()
		subset_df.to_string(cur_buffer, index=False)
		cur_buffer.seek(0)

		txt_buffers.append((cur_outfile_name, cur_buffer))

		print(f'{datetime.now()} {cur_outfile_name} txt binary created')

	return txt_buffers

def main():
	sql_scripts = file_type_in_dir(SQL_SCRIPTS_PATH, '.sql')

	for sql_script in sql_scripts:
		outfile_name = gen_file_name('', sql_script, '.sql', '.txt', '')
		txt_files = bq_to_txt(bq_client, f'{SQL_SCRIPTS_PATH}/{sql_script}', SLICE_BY_ROWS, outfile_name, [], True, False)
