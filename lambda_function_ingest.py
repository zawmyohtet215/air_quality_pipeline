import json
import os
from datetime import date, timedelta
from io import StringIO

import boto3
import openmeteo_requests
import pandas as pd
import requests
from retry_requests import retry

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

DEFAULT_LOCATIONS = {"Yangon": (16.87, 96.20),
                     "Mandalay": (21.98, 96.08),
                     "Bangkok": (13.75, 100.50),
                     "Chiang Mai": (18.79, 98.99),
                     "Mae Sot": (16.71, 98.57),}

s3_client = boto3.client("s3")

def get_date_range() -> tuple[str, str]:
    """Return (start_date, end_date) as YYYY-MM-DD strings"""
    yesterday = date.today() - timedelta(days=1)
    start = yesterday.strftime("%Y-%m-%d")
    end = yesterday.strftime("%Y-%m-%d")
    return start, end

def fetch_air_quality(locations: dict, start_date: str, end_date: str) -> pd.DataFrame:
    """Call the Open-Meteo Air Quality API for all locations and return one combined DataFrame."""    

    session = retry(requests.Session(), retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=session)

    params = {
        "latitude": [lat for lat, lon in locations.values()],
        "longitude": [lon for lat, lon in locations.values()],
        "hourly": ["pm10", "pm2_5"],
        "start_date": start_date,
        "end_date": end_date,}

    responses = openmeteo.weather_api(AIR_QUALITY_URL, params = params)

    all_dfs = []
    for name, response in zip(locations.keys(), responses):
        hourly = response.Hourly()

        hourly_data = {
            "date": pd.date_range(
                start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                freq = pd.Timedelta(seconds = hourly.Interval()),
                inclusive = "left",
            ),
            "location": name,
            "latitude": response.Latitude(),
            "longitude": response.Longitude(),
            "pm10": hourly.Variables(0).ValuesAsNumpy(),
            "pm2_5": hourly.Variables(1).ValuesAsNumpy()
            
        }

        all_dfs.append(pd.DataFrame(data = hourly_data))

    out_df = pd.concat(all_dfs, ignore_index=True)
    
    return out_df

def upload_to_s3(df: pd.DataFrame, bucket: str, prefix: str, end_date:str)-> str:
    """Write the DF as CSV directly to S3 and reutrn the S3 key."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    key = f"{prefix.rstrip('/')}/dt={end_date}/air_quality_{end_date}.csv"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
        )
    
    return key

def lambda_handler(event, context):
    bucket = os.environ.get("S3_BUCKET", "air-quality-bronze-zmh")
    prefix = os.environ.get("S3_PREFIX", "air_quality")
    
    locations = DEFAULT_LOCATIONS
    start_date, end_date =  get_date_range()
    
    df = fetch_air_quality(locations, start_date, end_date)
    key = upload_to_s3(df, bucket, prefix, end_date)
    
    result = {
        "statusCode": 200,
        "rows_written": len(df),
        "locations": list(locations.keys()),
        "date_range": {"start": start_date, "end": end_date},
        "s3_path": f"s3://{bucket}/{key}",
        }
    
    print(json.dumps(result))
    return result