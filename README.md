#

## Deploy

```shell
# enable APIs
gcloud services enable \
   cloudbuild.googleapis.com \
   run.googleapis.com \
   cloudfunctions.googleapis.com \
   eventarc.googleapis.com \
   bigquery.googleapis.com \
   pubsub.googleapis.com

# create the bucket
gsutil mb -l us-central1 -b on -p edgar-ai gs://edgar_666/

# deploy the functions
gcloud functions deploy load_master_idx \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --source ./load_master_idx \
  --entry-point gcs_event_handler \
  --trigger-bucket edgar_666  \
  --set-env-vars="BQ_DATASET_ID=edgar,BQ_TABLE_ID=master_idx,GCS_FOLDER_NAME=edgar_master_idx"

export SECRET_WORD=$(openssl rand -hex 16)
echo $SECRET_WORD
gcloud functions deploy download_edgar_idx \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --source ./download_edgar_idx \
  --entry-point http_handler \
  --trigger-bucket edgar_666  \
  --allow-unauthenticated \
  --set-env-vars="GCS_BUCKET_NAME=edgar_666,GCS_FOLDER_NAME=edgar_master_idx,SECRET_WORD=${SECRET_WORD}"

# run trigger script
python trigger_idx_load.py https://us-central1-edgar-ai.cloudfunctions.net/download_edgar_idx $SECRET_WORD

# check results
gsutil ls -l  gs://edgar_666/edgar_master_idx
bq query --use_legacy_sql=false "select count(*) from edgar-ai.edgar.master_idx where form_type='485BPOS'"

```
