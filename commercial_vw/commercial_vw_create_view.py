from pygcp.utils import *
from pygcp.bigquery import *
from pygcp.gcs_bucket import *
from pygcp.google_drive import *
from datetime import datetime, timezone, timedelta

JSON_KEYS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/utils/json-keys/gch-prod-dwh01-data-pipeline.json'
# JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/utils/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = JSON_KEYS_PATH
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)
bucket_client = storage.Client(credentials=credentials, project=credentials.project_id)

SQL_SCRIPTS_PATH = '/mnt/c/Users/Asus/Desktop/cloud-space-workspace/giant/gch-prod-dwh01/sql-scripts/landlord_report'
# SQL_SCRIPTS_PATH = '/home/yanzhe/gch-prod-dwh01/sql-scripts/landlord_report'

def month_table(view_name:str, src_dataset:str, src_table:str, dept:str):
	pass

def week_table(view_name:str, src_dataset:str, src_table:str, dept:str):
	query = f"""
	CREATE OR REPLACE VIEW `gch-prod-dwh01.commercial_vw.{view_name}`
	AS
	SELECT
		*
	FROM
		`gch-prod-dwh01.{src_dataset}.{src_table}`
	WHERE
		Dept LIKE '{dept}%'
		AND BizDate BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), YEAR) and CURRENT_DATE()
	"""

	job = bq_client.query(query)
	job.result()

def day_table(view_name:str, src_dataset:str, src_table:str, dept:str):
	query = f"""
	CREATE OR REPLACE VIEW `gch-prod-dwh01.commercial_vw.{view_name}`
	AS
	SELECT
		*
	FROM
		`gch-prod-dwh01.{src_dataset}.{src_table}`
	WHERE
		Dept LIKE '{dept}%'
		AND BizDate BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) and CURRENT_DATE()
	"""

	job = bq_client.query(query)
	job.result()

def month_table(view_name:str, src_dataset:str, src_table:str, dept:str):
	query = f"""
	CREATE OR REPLACE VIEW `gch-prod-dwh01.commercial_vw.{view_name}`
	AS
	SELECT
		*
	FROM
		`gch-prod-dwh01.{src_dataset}.{src_table}`
	WHERE
		Dept LIKE '{dept}%'
	"""

	job = bq_client.query(query)
	job.result()

with open('views.txt', 'r') as views_list:
	for view_name in views_list:
		view_name = view_name.strip()
		match view_name:
			case view_name if view_name.endswith('day'):
				week_table(f'{view_name}_tmp', src_dataset='commercial', src_table='possales_consign_tmp', dept=view_name[4])
			case view_name if view_name.endswith('week'):
				day_table(f'{view_name}_tmp', src_dataset='commercial', src_table='possales_consign_tmp', dept=view_name[4])
			case view_name if view_name.endswith('month'):
				month_table(f'{view_name}_tmp', src_dataset='commercial', src_table='', dept=view_name[4])
