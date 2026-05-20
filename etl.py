import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

# =========================
# CONFIG
# =========================

API_URL = "https://jsonplaceholder.typicode.com/users"

# AWS_ACCESS_KEY = "YOUR_ACCESS_KEY"
# AWS_SECRET_KEY = "YOUR_SECRET_KEY"
# AWS_REGION = "us-east-1"

S3_BUCKET_NAME = "github-s3-etl"
S3_FILE_NAME = f"etl/users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# =========================
# EXTRACT
# =========================

def extract_data(api_url):
    print("Extracting data from API...")

    response = requests.get(api_url)

    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code}")

    data = response.json()

    print(f"Extracted {len(data)} records")

    return data

# =========================
# TRANSFORM
# =========================

def transform_data(data):
    print("Transforming data...")

    df = pd.DataFrame(data)

    # Example transformations
    df = df[[
        "id",
        "name",
        "username",
        "email",
        "phone",
        "website"
    ]]

    # Convert column names to uppercase
    df.columns = [col.upper() for col in df.columns]

    print("Transformation completed")

    return df

# =========================
# LOAD TO S3
# =========================

def load_to_s3(df, bucket_name, file_name):
    print("Uploading file to S3...")

    # Convert dataframe to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    # Upload CSV to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=csv_buffer.getvalue()
    )

    print(f"File uploaded successfully to s3://{bucket_name}/{file_name}")

# =========================
# MAIN ETL PIPELINE
# =========================

def run_etl():
    try:
        # Extract
        raw_data = extract_data(API_URL)

        # Transform
        transformed_df = transform_data(raw_data)

        # Load
        load_to_s3(
            transformed_df,
            S3_BUCKET_NAME,
            S3_FILE_NAME
        )

        print("ETL pipeline completed successfully")

    except Exception as e:
        print(f"ETL pipeline failed: {e}")

# Run pipeline
if __name__ == "__main__":
    run_etl()