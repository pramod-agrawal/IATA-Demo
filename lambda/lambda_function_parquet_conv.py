import boto3
import pandas as pd
import io
from datetime import date, datetime

s3 = boto3.client("s3")
BUCKET="iata-data-lake-demo"

def lambda_handler(event, context):

    input_key = "staging/sales.csv"
    
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