import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

start_date = datetime.strptime('2024-07-02', '%Y-%m-%d').date()
end_pipeline_date = datetime.strptime('2024-08-02', '%Y-%m-%d').date()

with open('script.sql', 'r') as sql_script:
	query = ' '.join([line for line in sql_script]) # read the entire sql script

while start_date < end_pipeline_date:
	prev_interval = start_date

	'''
	- ETL process here
	- read from BQ
	- convert to file
	- export to designated sources

	use: bigquery.bq_to_excel, google_drive.local_exce_to_drive
	'''
	start_date += start_date.time_delta(days=6)
	query.replace(prev_interval, start_date) # replace the start date of the previous iteration
