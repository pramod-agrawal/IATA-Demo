import boto3
import pandas as pd
import io
from datetime import date, datetime

s3 = boto3.client("s3")
BUCKET="iata-data-lake-demo"

def lambda_handler(event, context):

    input_key = "staging/2m Sales Records.csv"
    
    obj = s3.get_object(Bucket=BUCKET, Key=input_key)

    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    for country, data in df.groupby("Country"):

        buffer = io.BytesIO()

        data.to_parquet(
            buffer,
            index=False,
            engine="pyarrow"
        )
        output_key = f"curated/country={country}/data.parquet"
        s3.put_object(
            Bucket=BUCKET,
            Key=output_key,
            Body=buffer.getvalue()
        )
    raw_key = f"raw/source=iata/dt={date.today()}/sales.zip"
    filename = raw_key.split("/")[-1]
    timestamp = datetime.now()
    archive_key = f"archive/{timestamp}_{filename}"    
    
    try:
        # archive original file
        archived = archive_s3_object(BUCKET, raw_key, archive_key)

        if not archived:
            raise Exception("Archive failed")
            
        # Delete original file
        delete_s3_object(BUCKET, raw_key)

    except Exception as e:
        print(f"Process failed: {str(e)}")
        
def archive_s3_object(BUCKET, raw_key, archive_key):

    try:
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": raw_key},
            Key=archive_key
        )
        print(f"Successfully archived: s3://{BUCKET}/{archive_key}")
        return True
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False
    
def delete_s3_object(bucket, raw_key):

    try:
        s3.delete_object(
            Bucket=bucket,
            Key=raw_key
        )
        print(f"Successfully deleted: s3://{bucket}/{raw_key}")

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
