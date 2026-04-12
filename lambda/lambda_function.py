import requests
import boto3
from datetime import date
import zipfile
import io

def lambda_handler(event, context):

    url = "https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/zip,application/octet-stream,*/*",
        "Referer": "https://eforexcel.com/",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }

    response = requests.get(url, headers=headers, stream=True)

    print("STATUS:", response.status_code)

    if response.status_code != 200:
        return {
            "error": "Download failed",
            "status": response.status_code,
            "text": response.text[:200]
        }

    s3 = boto3.client("s3")

    key = f"raw/source=iata/dt={date.today()}/sales.zip"

    s3.upload_fileobj(response.raw, "iata-data-lake-demo", key)

    #return {
    #    "status": response.status_code,
    #    "s3_path": key
    #}

##############################unzip###################
    BUCKET = "iata-data-lake-demo"
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    buffer = io.BytesIO(obj["Body"].read())

    z = zipfile.ZipFile(buffer)

    filename = z.namelist()[0]

    csv_data = z.read(filename)

    output_key = "staging/sales.csv"

    s3.put_object(
        Bucket=BUCKET,
        Key=output_key,
        Body=csv_data
    )