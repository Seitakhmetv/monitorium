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
    local TIMEOUT=${3:-60s}
    local MEMORY=${4:-256MB}

    gcloud functions deploy $NAME \
        --gen2 \
        --runtime=python310 \
        --region=$REGION \
        --source=. \
        --entry-point=$ENTRY \
        --trigger-http \
        --timeout=$TIMEOUT \
        --memory=$MEMORY \
        --set-env-vars $ENV_VARS \
        --service-account=$SA \
        --project=$PROJECT || {
            echo "ERROR: Failed to deploy $NAME"
            echo "$(date '+%Y-%m-%d %H:%M:%S') | FAILED | $NAME" >> $LOG_FILE
            exit 1
        }

    echo "$(date '+%Y-%m-%d %H:%M:%S') | SUCCESS | $NAME" >> $LOG_FILE
    echo "✓ $NAME deployed"
}

echo ""
echo "═══════════════════════════════════════════"
echo " Monitorium Functions Deploy"
echo " Project : $PROJECT"
echo " Region  : $REGION"
echo "═══════════════════════════════════════════"
echo ""

# deploy_function scraper-yfinance    scraper_yfinance_run  120s  256MB
# deploy_function scraper-worldbank   scraper_worldbank_run 120s  256MB
# deploy_function scraper-news        scraper_news_run      120s  256MB
# deploy_function scraper-kase        scraper_kase_run      120s  256MB
deploy_function run-silver          run_silver            3600s 256MB
deploy_function run-gold            run_gold              3600s 256MB
# deploy_function backfill            backfill              3600s 512MB
# deploy_function run-silver-backfill run_silver_backfill   3600s 256MB
# deploy_function backfill-kase       backfill_kase         3600s 256MB


echo ""
echo "✓ All functions deployed."
echo "  Log: $LOG_FILE"