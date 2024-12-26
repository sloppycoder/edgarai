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
gcloud functions deploy edgar_trigger \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --memory 1G \
  --timeout 180s \
  --source . \
  --trigger-topic edgar-test \
  --entry-point edgar_trigger \
  --set-env-vars="GCS_BUCKET_NAME=edgar_666"

# trigger
curl -m 190 -X POST $(gcloud functions describe edgar_trigger --format 'value(url)') \
-H "Authorization: bearer $(gcloud auth print-identity-token)" \
-H "Content-Type: application/json" \
-H "ce-id: 1234567890" \
-H "ce-specversion: 1.0" \
-H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
-H "ce-time: 2020-08-08T00:11:44.895529672Z" \
-H "ce-source: //pubsub.googleapis.com/projects/edgar-ai/topics/edgar-test" \
-d '{
  "message": {
    "attributes" :{
        "function":"load_master_idx",
        "year":"2021",
        "quarter": "1"
    },
    "data": "SGVsbG8gV29ybGQ=",
    "_comment": "data is base64 encoded string of '\''Hello World'\''"
  }
}'

curl -m 190 -X POST $(gcloud functions describe edgar_trigger --format 'value(url)') \
-H "Authorization: bearer $(gcloud auth print-identity-token)" \
-H "Content-Type: application/json" \
-H "ce-id: 1234567890" \
-H "ce-specversion: 1.0" \
-H "ce-type: google.cloud.pubsub.topic.v1.messagePublished" \
-H "ce-time: 2020-08-08T00:11:44.895529672Z" \
-H "ce-source: //pubsub.googleapis.com/projects/edgar-ai/topics/edgar-test" \
-d '{
  "message": {
    "attributes" :{
        "function":"chunk_one_filing",
        "filename":"edgar/data/1002427/0001133228-24-004879.txt"
    },
    "data": "SGVsbG8gV29ybGQ=",
    "_comment": "data is base64 encoded string of '\''Hello World'\''"
  }
}'

# check results
gsutil ls -lr  gs://edgar_666/cache
bq query --use_legacy_sql=false "select count(*) from edgar-ai.edgar.master_idx where accession_number is null"



```

## Notes
```requirements.txt``` is now handcrafted, not maintained by UV. This is because spacy 3.8.2 works on Mac but doesn't on Cloud Function. Version 3.7.2 works on Cloud Function but cannot run locally.
