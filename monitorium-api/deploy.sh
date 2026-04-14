#!/bin/bash
set -e

PROJECT=monitorium-491507
REGION=us-central1
SERVICE=monitorium-api
IMAGE=gcr.io/$PROJECT/$SERVICE

echo "── Building and pushing image..."
gcloud builds submit --tag $IMAGE --project $PROJECT

echo "── Deploying to Cloud Run..."
gcloud run deploy $SERVICE \
  --image $IMAGE \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --service-account monitorium-sa@$PROJECT.iam.gserviceaccount.com \
  --set-env-vars GCP_PROJECT_ID=$PROJECT,BQ_DATASET=monitorium_gold \
  --memory 512Mi \
  --min-instances 0 \
  --max-instances 10 \
  --project $PROJECT

echo "✓ $SERVICE deployed"
gcloud run services describe $SERVICE --region $REGION --project $PROJECT --format="value(status.url)"
