# python for bigquery
## Setup Enviroment to use python for bigquery
### 1. GCP SDK Install
Please refer to this page
https://cloud.google.com/sdk/docs/install
```
wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-437.0.1-darwin-arm.tar.gz?hl=ko
```
```
tar xvzf ./google-cloud-cli-437.0.1-darwin-arm.tar.gz
```
```
./google-cloud-sdk/install.sh
```
### 2. export GCP Service Key
Please refer to this page
https://cloud.google.com/iam/docs/keys-create-delete

Create GCP Service Account Key And Downloads At Your Location
```
echo "export GOOGLE_APPLICATION_CREDENTIALS="your_gcp_service_account_key_location"" >> ~/.bash_profile

source ~/.bash_profile

echo $GOOGLE_APPLICATION_CREDENTIALS
```

### 3. Install Bigquery Python Package
My Python Version is 3.9.6 On Apple Silicon
```
pip install google-cloud-bigquery
```
