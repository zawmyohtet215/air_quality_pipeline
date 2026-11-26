import pandas as pd
import boto3
import urllib.parse
import os
import io

# variables of s3
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "air-quality-bronze-zmh")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "air_quality")

SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "air-quality-silver-zmh")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "air_quality")

# clean & transform function
def clean_transform(df: pd.DataFrame)->pd.DataFrame:
    """Clean and transform the air quality data..."""
    ################### Clean ##############################
    
    # Data Cleaning
    # Parse timestamp, drop rows that fail to parse
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df = df.dropna(subset=["date"])
    
    # Standardize location strings
    df["location"] = df["location"].astype(str).str.strip()
    
    # Coerce numeric columns, drop rows where both pollutants are missing
    for col in ["latitude", "longitude", "pm10", "pm2_5"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["pm10", "pm2_5"], how="all")
    
    # Flag physically impossible readings instead of silently dropping
    # in order to the silver data auditable
    df["quality_flag"] = "ok"
    df.loc[ (df["pm10"]<0) | (df["pm2_5"]<0), "quality_flag" ] = "negative_reading"
    df.loc[ (df["pm2_5"] > df["pm10"]*1.5), "quality_flag" ] = "pm25_exceeds_pm10"
    # sanity check
    # pm2.5 is a subset of pm10 mass
            
    # Deduplicate on the natural key (returns/ overlapping pulls)
    df = df.drop_duplicates(subset=["date", "location"], keep="last")
    
    # Derive convenience columns silver consumers will want
    df["date_only"] = df["date"].dt.date.astype(str)
    df["hour"] = df["date"].dt.hour
    
    # Sort the df
    cleaned_df = df.sort_values(["location", "date"]).reset_index(drop=True)
    
    return cleaned_df
    

# Step 1: Connect to S3

s3 = boto3.client("s3")


def lambda_handler(event, context):
    
    results = []
    
    for record in event["Records"]:
        ################ Extract #############################
        # Read data from S3 Bronze Events
        src_bucket = record["s3"]["bucket"]["name"]
        src_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        obj = s3.get_object(Bucket=src_bucket, Key=src_key)
       
        # Convert as df
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        
        # Record rows in
        rows_in = len(df)
        
        ################# Data Cleaning #######################
        cleaned_df = clean_transform(df)
        
        ################# Transform Format ############################
        
        # Change format csv -> parquet
        # Partition by ingestion date for cheap Athena pruning later
        file_stub = os.path.splitext(os.path.basename(src_key))[0]
        out_key = f"{SILVER_PREFIX}/{file_stub}.parquet"
        
        buffer = io.BytesIO()
        cleaned_df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        
        ##################### Load ###############################
        s3.put_object(Bucket=SILVER_BUCKET,
                      Key=out_key,
                      Body=buffer.getvalue())
        
        # Record rows_out
        rows_out = len(cleaned_df)
        
        ##################### Log the results ########################
        results.append({
            "source": f"s3://{src_bucket}/{src_key}",
            "destination": f"s3://{SILVER_BUCKET}/{out_key}",
            "rows_in": rows_in,
            "rows_out": rows_out,
            "flagged_rows": int((cleaned_df["quality_flag"]!="ok").sum()),
            })
