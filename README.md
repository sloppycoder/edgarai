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

export CLOUDSDK_COMPUTE_REGION=us-central1

# create the bucket
gsutil mb -l $CLOUDSDK_COMPUTE_REGION -b on -p edgar-ai gs://edgar_666/

# deploy the functions
export SECRET_WORD=$(openssl rand -hex 16)
echo $SECRET_WORD
gcloud functions deploy load_edgar_idx \
  --gen2 \
  --region $CLOUDSDK_COMPUTE_REGION \
  --runtime python312 \
  --source . \
  --trigger-http \
  --entry-point load_idx_handler \
  --allow-unauthenticated \
  --set-env-vars="GCS_BUCKET_NAME=edgar_666,SECRET_WORD=${SECRET_WORD}"

# drop all files and table and start from scratch
gsutil -m rm -r  gs://edgar_666/cache
bq query --use_legacy_sql=false "drop table edgar-ai.edgar.master_idx"

# run trigger script
export FUNC_URL=$(gcloud functions describe load_edgar_idx --region $CLOUDSDK_COMPUTE_REGION --format 'value(url)')
curl -v $FUNC_URL\?year=2020\&qtr=1\&word=$SECRET_WORD

# check results
gsutil ls -lr  gs://edgar_666/cache
bq query --use_legacy_sql=false "select count(*) from edgar-ai.edgar.master_idx where accession_number is null"

```
