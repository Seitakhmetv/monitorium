#!/bin/bash
set -e

PROJECT=monitorium-491507
REGION=us-central1
SA=monitorium-sa@monitorium-491507.iam.gserviceaccount.com
ENV_VARS="GCS_BRONZE_BUCKET=monitorium-bronze"
LOG_FILE="deploy_functions.log"

deploy_function() {
    local NAME=$1
    local ENTRY=$2

    echo "── Deploying $NAME (entry: $ENTRY)..."

    gcloud functions deploy $NAME \
        --gen2 \
        --runtime=python310 \
        --region=$REGION \
        --source=. \
        --entry-point=$ENTRY \
        --trigger-http \
        --set-env-vars $ENV_VARS \
        --service-account=$SA \
        --project=$PROJECT || {
            echo "ERROR: Failed to deploy $NAME"
            echo "$(date '+%Y-%m-%d %H:%M:%S') | FAILED | $NAME | entry=$ENTRY" >> $LOG_FILE
            exit 1
        }

    echo "$(date '+%Y-%m-%d %H:%M:%S') | SUCCESS | $NAME | entry=$ENTRY" >> $LOG_FILE
    echo "✓ $NAME deployed"
}

echo ""
echo "═══════════════════════════════════════════"
echo " Monitorium Functions Deploy"
echo " Project : $PROJECT"
echo " Region  : $REGION"
echo "═══════════════════════════════════════════"
echo ""

deploy_function scraper-yfinance scraper_yfinance_run
deploy_function scraper-worldbank scraper_worldbank_run
deploy_function scraper-news scraper_news_run

echo ""
echo "✓ All functions deployed."
echo "  Log: $LOG_FILE"