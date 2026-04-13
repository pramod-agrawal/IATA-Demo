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
    
    filename = key.split("/")[-1]
    timestamp = datetime.now()
    archive_key = f"archive/{timestamp}_{filename}"    
    
    try:
        # archive original file
        archived = archive_s3_object(BUCKET, key, archive_key)

        if not archived:
            raise Exception("Archive failed")
            
        # Delete original file
        delete_s3_object(BUCKET, key)

    except Exception as e:
        print(f"Process failed: {str(e)}")

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
    csv_data = z.read(filename)

    s3.put_object(
        Bucket=BUCKET,
        Key=unzip_key,
        Body=csv_data
    )
    
def archive_s3_object(BUCKET, key, archive_key):

    try:
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": key},
            Key=archive_key
        )
        print(f"Successfully archived: s3://{BUCKET}/{archive_key}")
        return True
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False
    
def delete_s3_object(bucket, key):

    try:
        s3.delete_object(
            Bucket=bucket,
            Key=key
        )
        print(f"Successfully deleted: s3://{bucket}/{key}")

    except Exception as e:
        print(f"Unexpected error: {str(e)}")