from google.cloud import storage
import json

client = storage.Client()
bucket = client.bucket("monitorium-bronze")

# do this for each: raw/prices/, raw/worldbank/, raw/news/, raw/metadata/,
blob = bucket.blob("raw/news/2026-03-28.json")  # use your actual date
data = json.loads(blob.download_as_text())
print(json.dumps(data[:2], indent=2))