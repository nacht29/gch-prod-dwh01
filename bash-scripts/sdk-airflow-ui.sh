# SDK
gcloud auth login
gcloud auth list
gcloud config set project gch-prod-dwh01
gcloud compute start-iap-tunnel gchexpapp01p 8080     --local-host-port=localhost:3389     --zone=asia-southeast1-a