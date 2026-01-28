import os
import boto3
import datetime

s3 = boto3.client("s3")
BUCKET_NAME = os.environ["BUCKET_NAME"]

def lambda_handler(event, context):
    today = datetime.date.today().isoformat()
    download_path = "/tmp"

    # Tell Kaggle to use /tmp
    os.environ["KAGGLE_CONFIG_DIR"] = "/tmp/.kaggle"

    # Create Kaggle config directory
    os.makedirs("/tmp/.kaggle", exist_ok=True)

    with open("/tmp/.kaggle/kaggle.json", "w") as f:
        f.write(
            '{ "username": "%s", "key": "%s" }' % (
                os.environ["KAGGLE_USERNAME"],
                os.environ["KAGGLE_KEY"]
            )
        )

    os.chmod("/tmp/.kaggle/kaggle.json", 0o600)

    # Import AFTER setting config dir
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    # Download dataset
    api.dataset_download_files(
        "datasnaek/youtube-new",
        path=download_path,
        unzip=True
    )

    # Updated filename
    local_file = f"{download_path}/trending_TN.csv"
    s3_key = f"raw/ingestion_date={today}/trending_TN.csv"

    s3.upload_file(local_file, BUCKET_NAME, s3_key)

    return {
        "status": "success",
        "uploaded_to": f"s3://{BUCKET_NAME}/{s3_key}"
    }