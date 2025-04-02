import os
import calendar
import logging as log
from datetime import datetime
from google.cloud import bigquery as bq
from google.oauth2 import service_account

'''
Credentials
'''
JSON_KEYS_PATH = 'json-keys/gch-prod-dwh01-data-pipeline.json'
# JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

# set up Bucket credentials to load CSV to bucket
credentials = service_account.Credentials.from_service_account_file(JSON_KEYS_PATH)
bq_client = bq.Client(credentials=credentials, project=credentials.project_id)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SRP_PARENT_FOLDER_ID = '10VMUdmAQXPVZwxjB-LNjIC0bDgslkl0l'

'''
Local files
'''
PY_LOGS_DIR = '/mnt/c/Users/Asus/Desktop/py_log'
# PY_LOGS_DIR = '/home/yanzhe/py_log'
os.makedirs(PY_LOGS_DIR, exist_ok=True)

'''
Logging
'''
month = calendar.month_name[datetime.now().month]
year = datetime.now().year

# create log dir for current month/year
LOG_DIR = f'{PY_LOGS_DIR}/{year}/{month}'
os.makedirs(LOG_DIR, exist_ok=True)

# create log file name with timestamp
log_file_name = f'{datetime.now().strftime("%Y%m%d_%H%M%S")}_pylog.txt'
log_file_fullpath = f'{LOG_DIR}/{log_file_name}'

# config logging
log.basicConfig(
	filename=log_file_fullpath,
	level=log.INFO,
	format="%(asctime)s - %(levelname)s - %(message)s"
)

# log to console for debugging
console_handler = log.StreamHandler()
console_handler.setLevel(log.INFO)
log.getLogger().addHandler(console_handler)

# init task
log.info(f'{datetime.now()} srp_agg_data --initiated')