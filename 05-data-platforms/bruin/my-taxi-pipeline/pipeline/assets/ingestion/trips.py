"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

depends:
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    months = pd.date_range(start=start.replace(day=1), end=end, freq="ME")

    frames = []
    for (year, month) in [(d.year, d.month) for d in months]:
        for taxi_type in taxi_types:
            month_str = f"{month:02d}"
            url = f"{base_url}/{taxi_type}_tripdata_{year}-{month_str}.parquet"
            try:
                df = pd.read_parquet(url)
            except Exception:
                continue
            # Yellow uses tpep_*, green uses lpep_*
            pickup_col = "tpep_pickup_datetime" if "tpep_pickup_datetime" in df.columns else "lpep_pickup_datetime"
            dropoff_col = "tpep_dropoff_datetime" if "tpep_dropoff_datetime" in df.columns else "lpep_dropoff_datetime"
            out = pd.DataFrame({
                "pickup_datetime": df[pickup_col],
                "dropoff_datetime": df[dropoff_col],
                "pickup_location_id": df["PULocationID"],
                "dropoff_location_id": df["DOLocationID"],
                "fare_amount": df["fare_amount"],
                "payment_type": df["payment_type"],
                "taxi_type": taxi_type,
            })
            frames.append(out)

    if not frames:
        return pd.DataFrame()
    final_dataframe = pd.concat(frames, ignore_index=True)
    final_dataframe = final_dataframe[
        (final_dataframe["pickup_datetime"] >= start)
        & (final_dataframe["pickup_datetime"] < end)
    ]
    return final_dataframe