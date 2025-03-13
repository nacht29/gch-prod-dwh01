sudo apt-get update
sudo apt-get upgrade
sudo apt install pip -y
sudo apt install python3-venv -y
sudo apt install git -y
sudo apt install tmux -y
sudo apt install telnet -y

python3 -m venv myvenv
source myvenv/bin/activate

pip install google-cloud-bigquery
pip install --upgrade google-cloud google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2 google-cloud-storage pydrive
pip install google_cloud_bigquery_storage
pip instaa --upgrade db-dtypes xlrd pandas apache-airflow