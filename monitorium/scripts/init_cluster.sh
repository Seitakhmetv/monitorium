set -e

gsutil cp gs://monitorium-scripts/requirements.txt /tmp/requirements.txt
pip install -r /tmp/requirements.txt