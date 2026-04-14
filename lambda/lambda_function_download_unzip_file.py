import requests
import boto3
from datetime import date, datetime
import zipfile
import io

s3 = boto3.client("s3")

def lambda_handler(event, context):
    
    BUCKET = "iata-data-lake-demo"
    key = f"raw/source=iata/dt={date.today()}/sales.zip"
    unzip_key = "staging/sales.csv"

    url = "https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/zip,application/octet-stream,*/*",
        "Referer": "https://eforexcel.com/",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }

    response = requests.get(url, headers=headers, stream=True)

    if response.status_code != 200:
        return {
            "error": "Download failed",
            "status": response.status_code,
            "text": response.text[:200]
        }
    else:
        s3.upload_fileobj(response.raw, BUCKET, key)

    #unzip#
    unzip_raw_file(BUCKET, key, unzip_key)
    
    return {
        "status": response.status_code,
        "s3_zip_file_path": key,
        "s3_unzip_file_path": unzip_key
    }

############################## unzip raw file ###################
def unzip_raw_file(BUCKET, key, unzip_key):    
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    buffer = io.BytesIO(obj["Body"].read())

    z = zipfile.ZipFile(buffer)
    filename = z.namelist()[0]
    data = z.read(filename)

    s3.put_object(
        Bucket=BUCKET,
        Key=unzip_key,
        Body=data
    )