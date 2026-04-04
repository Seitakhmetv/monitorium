#!/bin/bash
set -e

# ── config ────────────────────────────────────────────────────────────────────
REGION=us-central1
PROJECT=monitorium-491507
BUCKET=gs://monitorium-scripts
LOG_FILE="deploy.log"
SCRIPT=$1

# ── validation ────────────────────────────────────────────────────────────────
if [ -z "$SCRIPT" ]; then
    echo "ERROR: No script provided."
    echo "Usage: bash deploy.sh transformation/silver_prices.py"
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

build_wheel() {
    local VERSION=$1
    echo "── Building wheel (version: $VERSION)..."

    sed -i.bak "s/version=\".*\"/version=\"$VERSION\"/" setup.py
    python3 -m build --wheel --no-isolation
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

    echo "── Uploading as latest..."
    gsutil cp $WHEEL_PATH $BUCKET/monitorium-latest-py3-none-any.whl

    echo "── Uploading script: $SCRIPT..."
    gsutil cp $SCRIPT $BUCKET/$SCRIPT

    echo "── Uploading requirements.txt..."
    gsutil cp requirements.txt $BUCKET/requirements.txt

    echo "── Creating Dataproc .env and uploading..."
    sed 's/ENV=local/ENV=dataproc/' .env | grep -v '^RUN_DATE' > .env.dataproc
    gsutil cp .env.dataproc $BUCKET/.env
    rm .env.dataproc

    echo "── Done uploading."
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

    check_existing_wheel $WHEEL_NAME

    build_wheel $VERSION || {
        echo "ERROR: Wheel build failed. Nothing was uploaded or submitted."
        log "FAILED" $VERSION $SCRIPT $WHEEL_NAME
        exit 1
    }
    
    WHEEL_PATH=$(ls dist/monitorium-*.whl | tail -1)
    WHEEL_NAME=$(basename $WHEEL_PATH)

    upload_artifacts dist/$WHEEL_NAME || {
        echo "ERROR: Upload failed. Old wheel may still be on GCS — do not submit manually."
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