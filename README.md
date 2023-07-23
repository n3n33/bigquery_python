# bigquery_python

## 1. GCP SDK Install
wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-437.0.1-darwin-arm.tar.gz?hl=ko

tar xvzf ./google-cloud-cli-437.0.1-darwin-arm.tar.gz

./google-cloud-sdk/install.sh

## export GCP Service Key
GCP Service Account Key Downloads

echo "export GOOGLE_APPLICATION_CREDENTIALS="your_gcp_service_account_key_location"" >> ~/.bash_profile

source ~/.bash_porfile

echo $GOOGLE_APPLICATION_CREDENTIALS
