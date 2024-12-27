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

gcloud config set compute/region us-central1

# create the bucket
gsutil mb -l us-central1 -b on -p edgar-ai gs://edgar_666/

# create pub/sub topic

# deploy the functions
gcloud functions deploy edgar_processor \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --memory 512M \
  --timeout 180s \
  --source . \
  --trigger-topic edgarai-request \
  --entry-point edgar_processor \
  --set-env-vars="GCS_BUCKET_NAME=edgar_666,RESPONSE_TOPIC=edgarai-response"

gcloud functions deploy trigger_chunk_filing \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --source . \
  --trigger-http \
  --no-allow-unauthenticated \
  --entry-point trigger_chunk_filing \
  --set-env-vars="GCS_BUCKET_NAME=edgar_666,REQUEST_TOPIC=edgarai-request,RESPONSE_TOPIC=edgarai-response"

# trigger=
python scripts/trigger.py "idx|2020|1" -w
python scripts/trigger.py "chunk|edgar/data/1002427/0001133228-24-004879.txt" -w

# check results
gsutil ls -lr  gs://edgar_666/cache
bq query --use_legacy_sql=false "select count(*) from edgar-ai.edgar.master_idx where accession_number is null"



```

## Notes
```requirements.txt``` is now handcrafted, not maintained by UV. This is because spacy 3.8.2 works on Mac but doesn't on Cloud Function. Version 3.7.2 works on Cloud Function but cannot run locally.
