#!/bin/bash
set -e

# ── config ────────────────────────────────────────────────────────────────────
CLUSTER=monitorium-cluster
REGION=us-central1
PROJECT=monitorium-491507
BUCKET=gs://monitorium-scripts
LOG_FILE="deploy.log"
SCRIPT=$1

# ── validation ────────────────────────────────────────────────────────────────
if [ -z "$SCRIPT" ]; then
    echo "ERROR: No script provided."
    echo "Usage: bash deploy.sh transformation/gold_dim_date.py"
    exit 1
fi

if [ ! -f "$SCRIPT" ]; then
    echo "ERROR: Script '$SCRIPT' not found locally."
    exit 1
fi

# ── helpers ───────────────────────────────────────────────────────────────────
get_version() {
    git rev-parse --short HEAD
}

log() {
    local STATUS=$1
    local VERSION=$2
    local SCRIPT=$3
    local WHEEL=$4
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $STATUS | version=$VERSION | script=$SCRIPT | wheel=$WHEEL" >> $LOG_FILE
}

cluster_exists() {
    gcloud dataproc clusters describe $CLUSTER \
        --region=$REGION \
        --project=$PROJECT \
        &>/dev/null
}

create_cluster() {
    echo "── Uploading requirements.txt and init script..."
    gsutil cp requirements.txt $BUCKET/requirements.txt
    gsutil cp scripts/init_cluster.sh $BUCKET/scripts/init_cluster.sh

    echo "── Creating Dataproc cluster: $CLUSTER..."
    gcloud dataproc clusters create $CLUSTER \
        --region=$REGION \
        --zone=${REGION}-a \
        --master-machine-type=n1-standard-2 \
        --worker-machine-type=n1-standard-2 \
        --num-workers=2 \
        --image-version=2.1-debian11 \
        --optional-components=JUPYTER \
        --enable-component-gateway \
        --initialization-actions=$BUCKET/scripts/init_cluster.sh \
        --project=$PROJECT

    echo "── Cluster created."
}

ensure_cluster() {
    if cluster_exists; then
        echo "── Cluster '$CLUSTER' already exists. Skipping creation."
    else
        echo "── Cluster '$CLUSTER' not found."
        create_cluster
    fi
}

build_wheel() {
    local VERSION=$1
    echo "── Building wheel (version: $VERSION)..."

    sed -i.bak "s/version=\".*\"/version=\"$VERSION\"/" setup.py
    python -m build --wheel --no-isolation
    mv setup.py.bak setup.py

    WHEEL_PATH=$(ls dist/monitorium-*.whl | tail -1)
    echo "── Built: $WHEEL_PATH"
}

check_existing_wheel() {
    local WHEEL_NAME=$1
    if gsutil -q stat "$BUCKET/$WHEEL_NAME" 2>/dev/null; then
        echo ""
        echo "⚠️  WARNING: A wheel with this commit hash already exists in GCS:"
        echo "   $BUCKET/$WHEEL_NAME"
        echo "   This means you are redeploying without new commits."
        read -p "   Continue anyway? (y/n): " CONFIRM
        if [ "$CONFIRM" != "y" ]; then
            echo "Aborted."
            exit 0
        fi
    fi
}

upload_artifacts() {
    local WHEEL_PATH=$1
    local WHEEL_NAME=$(basename $WHEEL_PATH)

    echo "── Uploading wheel: $WHEEL_NAME..."
    gsutil cp $WHEEL_PATH $BUCKET/$WHEEL_NAME

    echo "── Uploading script: $SCRIPT..."
    gsutil cp $SCRIPT $BUCKET/$SCRIPT

    echo "── Uploading requirements.txt..."
    gsutil cp requirements.txt $BUCKET/requirements.txt

    echo "── Uploading .env..."
    gsutil cp .env $BUCKET/.env
}

submit_job() {
    local WHEEL_NAME=$1

    echo ""
    echo "── Submitting job to Dataproc..."
    echo "   Script  : $SCRIPT"
    echo "   Wheel   : $WHEEL_NAME"
    echo "   Cluster : $CLUSTER"
    echo ""

    gcloud dataproc jobs submit pyspark \
        $BUCKET/$SCRIPT \
        --cluster=$CLUSTER \
        --region=$REGION \
        --py-files=$BUCKET/$WHEEL_NAME \
        --files=$BUCKET/.env \
        --project=$PROJECT
}

# ── main ──────────────────────────────────────────────────────────────────────
main() {
    VERSION=$(get_version)
    echo ""
    echo "═══════════════════════════════════════════"
    echo " Monitorium Deploy"
    echo " Commit  : $VERSION"
    echo " Script  : $SCRIPT"
    echo "═══════════════════════════════════════════"
    echo ""

    WHEEL_NAME="monitorium-${VERSION}-py3-none-any.whl"

    ensure_cluster

    check_existing_wheel $WHEEL_NAME

    build_wheel $VERSION || {
        echo "ERROR: Wheel build failed. Nothing was uploaded or submitted."
        log "FAILED" $VERSION $SCRIPT $WHEEL_NAME
        exit 1
    }

    upload_artifacts dist/$WHEEL_NAME || {
        echo "ERROR: Upload failed. Old wheel may still be on GCS — do not submit manually."
        log "FAILED" $VERSION $SCRIPT $WHEEL_NAME
        exit 1
    }

    submit_job $WHEEL_NAME || {
        echo "ERROR: Dataproc job submission failed."
        echo "Wheel submitted: $BUCKET/$WHEEL_NAME"
        echo "Check logs: gcloud dataproc jobs list --region=$REGION --project=$PROJECT"
        log "FAILED" $VERSION $SCRIPT $WHEEL_NAME
        exit 1
    }

    log "SUCCESS" $VERSION $SCRIPT $WHEEL_NAME
    echo ""
    echo "✓ Deployment complete."
    echo "  Wheel   : $WHEEL_NAME"
    echo "  Log     : $LOG_FILE"
}

main "$@"