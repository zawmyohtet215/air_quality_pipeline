# Aggregate daily summary of air quality data in S3 bucket (silver)
# Send the result to the S3 bucket (gold)

import pandas as pd
import boto3
import urllib.parse
import os
import io

# variables of S3
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", "air-quality-silver-zmh")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "air_quality")

GOLD_BUCKET = os.environ.get("GOLD_BUCKET", "air-quality-gold-zmh")
GOLD_PREFIX = os.environ.get("GOLD_PREFIX", "air_quality")

# aggregate function
def summarize_df(silver_df: pd.DataFrame)->pd.DataFrame:
    """Aggregate the air quality data..."""
    
    # Calculate aggregations
    gold_df = silver_df.groupby(["date_only", "location"]).agg(
        avg_pm10=("pm10", "mean"),
        min_pm10=("pm10", "min"),
        max_pm10=("pm10", "max"),
        avg_pm2_5=("pm2_5", "mean"),
        min_pm2_5=("pm2_5", "min"),
        max_pm2_5=("pm2_5", "max"),
        total_readings=("hour", "count"),
        flagged_count=("quality_flag", lambda x: (x != "ok").sum())).reset_index()
    
    # Round the results
    for col in ["avg_pm10", "min_pm10", "max_pm10", "avg_pm2_5", "min_pm2_5", "max_pm2_5"]:
        gold_df[col]= gold_df[col].round(2)
        
    return gold_df
        
# Step 1: Connect to S3

s3 = boto3.client("s3")


def lambda_handler(event, context):
    
    results = []
    
    for record in event["Records"]:
        ################ Extract #############################
        
        # Read data from S3 Silver Event (No need to read files, just read the event)
        src_bucket = record["s3"]["bucket"]["name"]
        src_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        obj = s3.get_object(Bucket=src_bucket, Key=src_key)
       
        # Convert as df
        silver_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        
        ################# Aggregate Data #######################
        gold_df_agg = summarize_df(silver_df)
        
        ################# Transform Format ############################
        
        # Change format csv -> parquet
        # Partition by ingestion date for cheap Athena pruning later
        file_stub = os.path.splitext(os.path.basename(src_key))[0]
        out_key = f"{GOLD_PREFIX}/daily_summary_{file_stub}.parquet"
        
        buffer = io.BytesIO()
        gold_df_agg.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        
        ##################### Load ###############################
        s3.put_object(Bucket=GOLD_BUCKET,
                      Key=out_key,
                      Body=buffer.getvalue())
        
        ##################### Log the results ########################
        results.append({
            "source": f"s3://{src_bucket}/{src_key}",
            "destination": f"s3://{GOLD_BUCKET}/{out_key}",
            "summary_rows": len(gold_df_agg),
            })
        
        return {"statusCode": 200, "body": results}