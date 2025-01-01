# BigQuery Helper Functions

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

# deploy the functions in parallel
gcloud functions deploy edgar_processor \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --memory 512M \
  --timeout 900 \
  --source . \
  --trigger-http \
  --no-allow-unauthenticated \
  --entry-point edgar_processor \
  --set-env-vars="GCS_BUCKET_ID=edgar_666" \
  &

PID1=$!

gcloud functions deploy get_most_relevant_chunks \
  --gen2 \
  --region us-central1 \
  --runtime python312 \
  --source . \
  --trigger-http \
  --no-allow-unauthenticated \
  --entry-point get_most_relevant_chunks \
  &

PID2=$!

wait $PID1 $PID2

```shell

# Do the create remote function setup in BigQuery and
# then invoke function using SQL
select `edgar`.trigger_processor('load_master_idx', '2020|1');

 select `edgar`.trigger_processor(
      'chunk_one_filing',
      '1002427|edgar/data/1002427/0001133228-24-004879.txt')
  ;


gsutil ls -lr  gs://edgar_666/cache

```

## Notes
```requirements.txt``` is now handcrafted, not maintained by UV. This is because spacy 3.8.2 works on Mac but doesn't on Cloud Function. Version 3.7.2 works on Cloud Function but cannot run locally.

## IAM roles required
Ensure the service account or user have the following roles in order to do development.

```shell
gcloud projects get-iam-policy <project_id> \
    --flatten="bindings[].members" \
    --format="table(bindings.role)" \
    --filter="bindings.members:serviceAccount:<service_account_email>"
ROLE
roles/bigquery.dataOwner
roles/bigquery.jobUser
roles/cloudfunctions.developer
roles/compute.networkViewer
roles/iam.securityReviewer
roles/iam.serviceAccountUser
roles/pubsub.publisher
roles/pubsub.subscriber
roles/run.admin
roles/storage.objectAdmin
```
