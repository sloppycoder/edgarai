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

# deploy the function
gcloud functions deploy load_master_idx \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --source ./load_master_idx \
  --entry-point gcs_event_handler \
  --trigger-bucket edgar_666  \
  --set-env-vars="BQ_DATASET_ID=edgar,BQ_TABLE_ID=master_idx,GCS_FOLDER_NAME=edgar_master_idx/"

```
