import os
import gcsfs
import calendar
import logging as log
import pandas as pd
from datetime import date, datetime, timezone, timedelta
from google.cloud import bigquery as bq
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.api_core.exceptions import Forbidden, NotFound

'''
CREDENTIALS	
'''
# JSON_KEYS_PATH = 'json-keys/gch-prod-dwh01-data-pipeline.json'
JSON_KEYS_PATH = '/home/yanzhe/gch-prod-dwh01/json-keys/gch-prod-dwh01-data-pipeline.json'
SERVICE_ACCOUNT = f'{JSON_KEYS_PATH}'

'''
LOCAL FILE PATHS
'''
# PY_LOGS_DIR = '/mnt/c/Users/Asus/Desktop/py_log'
PY_LOGS_DIR = '/home/yanzhe/py_log'
os.makedirs(PY_LOGS_DIR, exist_ok=True)

'''
GOOGLE DRIVE PARAMS
'''
SCOPES = ['https://www.googleapis.com/auth/drive']

POSSALES_RL_FOLDER_ID = '1LYITa9mHJZXQyC21_75Ip8_oMwBanfcF' # use this for the actual prod
# POSSALES_RL_FOLDER_ID = '1iQDbpxsqa8zoEIREJANEWau6HEqPe7hF' # GCH Report > Supply Chain (mock drive)

'''
OUTPUT FILE CONFIG
'''
SLICE_BY_ROWS = 500000 - 1

DEPARTMENTS = {
	'1': '1 - GROCERY',
	'2': '2 - FRESH',
	'3': '3 - PERISHABLES',
	'4': '4 - NON FOODS',
	'5': '5 - HEALTH & BEAUTY',
	'6': '6 - GMS'
}

bucket_fs = gcsfs.GCSFileSystem()
