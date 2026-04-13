import boto3
import pandas as pd
import io
from datetime import date
from datetime import datetime

s3 = boto3.client("s3")
BUCKET="iata-data-lake-demo"

def lambda_handler(event, context):

    key = "staging/sales.csv"

    obj = s3.get_object(Bucket=BUCKET, Key=key)

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

    #return {"status": "partition complete"}

    today = datetime.today().strftime("%Y-%m-%d")
    raw_key = f"raw/source=iata/dt={date.today()}/sales.zip"
    filename = raw_key.split("/")[-1]
    timestamp = datetime.now()
    archive_key = f"archive/{timestamp}_{filename}"
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": raw_key},
        Key=archive_key
    )

    # Delete original file
    s3.delete_object(
        Bucket=BUCKET,
        Key=raw_key
    )