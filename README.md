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

# prepare secret word for triggering, as a crude security mechanism
openssl rand -hex 16 > .secret_word
export SECRET_WORD=$(cat .secret_word)
echo $SECRET_WORD

# deploy the functions
gcloud functions deploy load_edgar_idx \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --timeout 180s \
  --source . \
  --trigger-http \
  --entry-point load_idx_handler \
  --allow-unauthenticated \
  --set-env-vars="GCS_BUCKET_NAME=edgar_666,SECRET_WORD=${SECRET_WORD}"

# drop all files and table and start from scratch
gsutil -m rm -r  gs://edgar_666/cache
bq query --use_legacy_sql=false "drop table edgar-ai.edgar.master_idx"

# run trigger script
curl $(gcloud functions describe load_edgar_idx --format 'value(url)')\?year=2020\&qtr=1\&word=$SECRET_WORD

# check results
gsutil ls -lr  gs://edgar_666/cache
bq query --use_legacy_sql=false "select count(*) from edgar-ai.edgar.master_idx where accession_number is null"


# chunk
gcloud functions deploy chunk_one_filing \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --memory 1G \
  --timeout 180s \
  --source . \
  --trigger-http \
  --entry-point chunk_one_filing \
  --allow-unauthenticated \
  --set-env-vars="GCS_BUCKET_NAME=edgar_666,SECRET_WORD=${SECRET_WORD}"

# test
curl $(gcloud functions describe chunk_one_filing --format 'value(url)')\?filename=edgar/data/1002427/0001133228-24-004879.txt\&word=$SECRET_WORD

```
