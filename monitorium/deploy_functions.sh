#!/bin/bash
set -e

PROJECT=monitorium-491507
REGION=us-central1
SA=monitorium-sa@monitorium-491507.iam.gserviceaccount.com
LOG_FILE="deploy_functions.log"

ENV_VARS="GCP_PROJECT_ID=monitorium-491507,\
GCS_BRONZE_BUCKET=monitorium-bronze,\
GCS_SILVER_BUCKET=monitorium-silver,\
GCS_SCRIPTS_BUCKET=monitorium-scripts,\
BQ_DATASET=monitorium_gold"

deploy_function() {
    local NAME=$1
    local ENTRY=$2
    local TIMEOUT=${3:-120s}
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

# Pass a specific function name as argument to deploy just one, e.g.:
#   ./deploy_functions.sh scraper-yfinance
# Leave empty to deploy all.

TARGET=${1:-all}

deploy_one() {
    case $1 in
        scraper-yfinance)   deploy_function scraper-yfinance    scraper_yfinance_run   120s  256MB ;;
        scraper-worldbank)  deploy_function scraper-worldbank   scraper_worldbank_run  120s  256MB ;;
        scraper-news)       deploy_function scraper-news        scraper_news_run       300s  256MB ;;
        scraper-kase)       deploy_function scraper-kase        scraper_kase_run       120s  256MB ;;
        run-silver)         deploy_function run-silver          run_silver             3600s 256MB ;;
        run-gold)           deploy_function run-gold            run_gold               3600s 256MB ;;
        backfill)           deploy_function backfill            backfill               3600s 512MB ;;
        run-silver-backfill) deploy_function run-silver-backfill run_silver_backfill   3600s 1024MB ;;
        run-gold-backfill)  deploy_function run-gold-backfill   run_gold_backfill      3600s 1024MB ;;
        full-backfill)      deploy_function full-backfill       full_backfill          3600s 1024MB ;;
        *) echo "Unknown function: $1"; exit 1 ;;
    esac
}

if [ "$TARGET" = "all" ]; then
    for fn in scraper-yfinance scraper-worldbank scraper-news scraper-kase \
              run-silver run-gold backfill run-silver-backfill run-gold-backfill full-backfill; do
        deploy_one $fn
    done
else
    deploy_one $TARGET
fi

echo ""
echo "✓ Done.  Log: $LOG_FILE"
