#!/bin/bash
set -e

# ── config ────────────────────────────────────────────────────────────────────
REGION=us-central1
PROJECT=monitorium-491507
BUCKET=gs://monitorium-scripts
LOG_FILE="deploy.log"
SCRIPT=$1

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
    # Clean dist first so tail -1 always gets the new wheel
    rm -f dist/monitorium-*.whl
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

upload_wheel() {
    local WHEEL_PATH=$1
    local WHEEL_NAME=$(basename $WHEEL_PATH)

    echo "── Uploading wheel: $WHEEL_NAME..."
    gsutil cp $WHEEL_PATH $BUCKET/$WHEEL_NAME

    echo "── Uploading as latest..."
    gsutil cp $WHEEL_PATH $BUCKET/monitorium-latest-py3-none-any.whl

    echo "── Building ingestion.zip for Dataproc python_file_uris..."
    # Dataproc Serverless only supports .py/.zip/.egg in python_file_uris — not .whl.
    # Zip the ingestion/ package so executors can import it directly.
    rm -f ingestion.zip
    zip -r ingestion.zip ingestion/
    echo "── Uploading ingestion.zip..."
    gsutil cp ingestion.zip $BUCKET/ingestion.zip
    rm -f ingestion.zip

    echo "── Uploading requirements.txt..."
    gsutil cp requirements.txt $BUCKET/requirements.txt

    echo "── Creating Dataproc .env and uploading..."
    sed 's/ENV=local/ENV=dataproc/' .env \
        | grep -v '^RUN_DATE' \
        | grep -v '^GOOGLE_APPLICATION_CREDENTIALS' \
        > .env.dataproc
    gsutil cp .env.dataproc $BUCKET/.env
    rm .env.dataproc

    echo "── Wheel upload done."
}

upload_script() {
    local SCRIPT=$1
    echo "── Uploading script: $SCRIPT..."
    gsutil cp $SCRIPT $BUCKET/$SCRIPT
    echo "── Script upload done."
}

upload_all_scripts() {
    echo "── Uploading all transformation scripts..."
    for f in transformation/*.py; do
        gsutil cp $f $BUCKET/$f
        echo "   ✓ $f"
    done
    echo "── All scripts uploaded."
}

# ── main ──────────────────────────────────────────────────────────────────────
main() {
    VERSION=$(get_version)
    WHEEL_NAME="monitorium-${VERSION}-py3-none-any.whl"

    echo ""
    echo "═══════════════════════════════════════════"
    echo " Monitorium Deploy"
    echo " Commit  : $VERSION"
    echo " Script  : ${SCRIPT:-none (wheel + all scripts)}"
    echo "═══════════════════════════════════════════"
    echo ""

    # Validate script path if provided
    if [ -n "$SCRIPT" ] && [ "$SCRIPT" != "--wheel-only" ] && [ ! -f "$SCRIPT" ]; then
        echo "ERROR: Script '$SCRIPT' not found locally."
        exit 1
    fi

    check_existing_wheel $WHEEL_NAME

    build_wheel $VERSION || {
        echo "ERROR: Wheel build failed."
        log "FAILED" $VERSION "${SCRIPT:-wheel-only}" $WHEEL_NAME
        exit 1
    }

    WHEEL_PATH=$(ls dist/monitorium-*.whl | tail -1)
    WHEEL_NAME=$(basename $WHEEL_PATH)

    upload_wheel $WHEEL_PATH || {
        echo "ERROR: Wheel upload failed."
        log "FAILED" $VERSION "${SCRIPT:-wheel-only}" $WHEEL_NAME
        exit 1
    }

    if [ "$SCRIPT" == "--wheel-only" ]; then
        : # wheel-only mode — skip scripts
    elif [ -n "$SCRIPT" ]; then
        # Upload a single named script
        upload_script $SCRIPT || {
            echo "ERROR: Script upload failed."
            log "FAILED" $VERSION $SCRIPT $WHEEL_NAME
            exit 1
        }
    else
        # Default: upload all transformation scripts
        upload_all_scripts || {
            echo "ERROR: Script upload failed."
            log "FAILED" $VERSION "all-scripts" $WHEEL_NAME
            exit 1
        }
    fi

    log "SUCCESS" $VERSION "${SCRIPT:-all-scripts}" $WHEEL_NAME
    echo ""
    echo "✓ Deployment complete."
    echo "  Wheel : $WHEEL_NAME"
    echo "  Log   : $LOG_FILE"
}

main "$@"