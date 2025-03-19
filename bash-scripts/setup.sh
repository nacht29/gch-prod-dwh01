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
    pydrive db-dtypes xlrd pandas openpyxl apache-airflow apache-airflow[gcp] \
    google-cloud-bigquery-storage


# Run py script
echo "executing exapp_pipeline"
# /home/yanzhe/myvenv/bin/activate

/home/yanzhe/myvenv/bin/python /home/yanzhe/gch-prod-dwh01/exapp_pipeline.py
/home/yanzhe/myvenv/bin/python /home/yanzhe/gch-prod-dwh01/exapp_pipeline_prod.py

#/home/yanzhe/myvenv/bin/deactivate


# =================================================================================================================================

# 6. Set up Airflow directories
# mkdir -p /home/yanzhe/airflow/dags

# 7. Initialize Airflow database
# /home/yanzhe/myvenv/bin/airflow db init

# 8. Copy pipeline script to Airflow DAGs
# cp /home/yanzhe/gchexapp01p/exapp_pipeline.py /home/yanzhe/airflow/dags/

# 9. Kill any existing tmux sessions for Airflow
# /usr/bin/tmux kill-session -t airflow_web 2>/dev/null || true
# /usr/bin/tmux kill-session -t airflow_sched 2>/dev/null || true

# 10. Start Airflow services in tmux sessions
# /usr/bin/tmux new-session -d -s airflow_web "/home/yanzhe/myvenv/bin/airflow webserver -p 8080"
# /usr/bin/tmux new-session -d -s airflow_sched "/home/yanzhe/myvenv/bin/airflow scheduler"

#echo "Setup complete"
#echo "Airflow webserver running on port 8080"
#echo "Access Airflow at http://$(/usr/bin/curl -s ifconfig.me):8080"