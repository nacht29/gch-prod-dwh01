# sudo apt-get update
# sudo apt-get upgrade
# sudo apt install pip -y
# sudo apt install python3-venv -y
# sudo apt install git -y
# sudo apt install tmux -y
# sudo apt install telnet -y

# python3 -m venv myvenv
# source myvenv/bin/activate

# pip install google-cloud-bigquery
# pip install --upgrade google-cloud google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2 google-cloud-storage pydrive
# pip install google_cloud_bigquery_storage
# pip install --upgrade db-dtypes xlrd pandas apache-airflow

#!/bin/bash

# 1. System Update & Upgrade
sudo apt-get update -y
sudo apt-get upgrade -y

# 2. Install System Dependencies
sudo apt-get install -y \
    pip \
    python3-venv \
    git \
    tmux \
    telnet

# 3. Set Timezone
sudo timedatectl set-timezone "Asia/Singapore"

# 4. Create Python Virtual Environment
python3 -m venv ~/myvenv
source ~/myvenv/bin/activate

# 5. Install Python Packages
pip install --upgrade \
    google-cloud-bigquery \
    google-api-python-client \
    google-auth \
    google-auth-oauthlib \
    google-auth-httplib2 \
    google-cloud-storage \
    pydrive \
    db-dtypes \
    xlrd \
    pandas \
    apache-airflow \
    google-cloud-bigquery-storage

# 6. Set up Airflow Directories
mkdir -p ~/airflow/dags

# 7. Initialize Airflow Database
airflow db init

# 8. Copy Pipeline Script to DAGs
cp /home/yanzhe/gchexapp01p/exapp_pipeline.py ~/airflow/dags/

# 9. Set up Google Cloud Credentials
export GOOGLE_APPLICATION_CREDENTIALS="/home/yanzhe/gchexapp01p/json-keys/gch-prod-dwh01-data-pipeline.json"

# 10. Kill existing tmux sessions if they exist
tmux kill-session -t airflow_web 2>/dev/null || true
tmux kill-session -t airflow_sched 2>/dev/null || true

# 11. Start Airflow Services in tmux
tmux new-session -d -s airflow_web "airflow webserver -p 8080"
tmux new-session -d -s airflow_sched "airflow scheduler"

echo "Setup complete"
echo "Airflow webserver running on port 8080"
echo "Access Airflow at http://$(curl -s ifconfig.me):8080"