import json
import dataclasses
import pandas as pd

from typing import Optional
from dataclasses import dataclass


@dataclass
class Ride:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    #passenger_count: int
    passenger_count: Optional[int]
    trip_distance: float
    tip_amount: float
    total_amount: float
    

def ride_from_row(row):
    #def safe_int(value):
    #    return int(value) if not pd.isna(value) else 0
    
    def safe_int_or_none(value):
        if pd.isna(value):
            return None
        return int(value)

    return Ride(
        #lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        #lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        #PULocationID=safe_int(row['PULocationID']),
        #DOLocationID=safe_int(row['DOLocationID']),
        #passenger_count=safe_int(row['passenger_count']),
        PULocationID=safe_int_or_none(row['PULocationID']),
        DOLocationID=safe_int_or_none(row['DOLocationID']),
        passenger_count=safe_int_or_none(row['passenger_count']),
        trip_distance=float(row['trip_distance']) if not pd.isna(row['trip_distance']) else 0.0,
        tip_amount=float(row['tip_amount']) if not pd.isna(row['tip_amount']) else 0.0,
        total_amount=float(row['total_amount']) if not pd.isna(row['total_amount']) else 0.0,
    )


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
