#!/bin/bash

# Set PATH for commands
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Google Cloud credentials ENV
export GOOGLE_APPLICATION_CREDENTIALS="/home/yanzhe/gchexapp01p/json-keys/gch-prod-dwh01-data-pipeline.json"

# sudo update upgrade
sudo /usr/bin/apt-get update -y && sudo /usr/bin/apt-get upgrade -y

# Install system dependencies
sudo /usr/bin/apt-get install -y pip python3-venv git tmux telnet

# Set timezone (UTC +8)
sudo /usr/bin/timedatectl set-timezone "Asia/Singapore"

# Create Python VENV
if [ ! -d "/home/yanzhe/myvenv" ]; then
	/usr/bin/python3 -m venv /home/yanzhe/myvenv
fi

# Upgrade pip
/home/yanzhe/myvenv/bin/python -m pip install --upgrade pip

# Install Python packages in VENV
/home/yanzhe/myvenv/bin/python -m pip install --upgrade \
	google-cloud-bigquery google-api-python-client google-auth \
	google-auth-oauthlib google-auth-httplib2 google-cloud-storage \
	pydrive db-dtypes xlrd pandas openpyxl xlsxwriter\
	google-cloud-bigquery-storage
	#apache-airflow apache-airflow[gcp]

/home/yanzhe/myvenv/bin/python -m pip install --index-url https://test.pypi.org/simple/ --no-deps pygcp==1.1.1

# Run py script
echo "executing landlord_report_pipeline_dev"

/home/yanzhe/myvenv/bin/python /home/yanzhe/gch-prod-dwh01/landlord_report/landlord_report_pipeline_dev.py> /var/log/landlord_report_pipeline.log 2>&1

# if [ $? -ne 0 ]; then
# 	echo "landlord_report_pipeline_dev failed. Sending mail alert..."

# 	ERROR_TRACEBACK=$(cat /var/log/landlord_report_pipeline.log)

# 	/home/yanzhe/myvenv/bin/python /home/yanzhe/gch-prod-dwh01/landlord_report/alert.py "$ERROR_TRACEBACK"
# fi
