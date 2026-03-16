import sys
import psycopg2
from kafka import KafkaConsumer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from models_green import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-database',
    value_deserializer=ride_deserializer
)

greater_than_five = 0
count = 0

try:
    for message in consumer:
        ride = message.value
        count += 1
        if count % 1000 == 0:
            print(f"Read {count} rows...")
        if float(ride.trip_distance) > 5.0:
            greater_than_five += 1

    consumer.close()

except KeyboardInterrupt:
    # When pressing Ctrl+C
    print("\n[!] Running interruption detected. Closing the consumer safely...")

finally:
    consumer.close()
    print("Kafka connection closed")

    print(f"Total messages: {count}")
    print(f"With trip distance > 5: {greater_than_five}")

#conn = psycopg2.connect(
#    host='localhost',
#    port=5432,
#    database='postgres',
#    user='postgres',
#    password='postgres'
#)
#
#conn.autocommit = True
#cur = conn.cursor()
#
#print(f"Listening to {topic_name} and writing to PostgreSQL...")
#
#count = 0
#for message in consumer:
#    ride = message.value
#    cur.execute(
#        """INSERT INTO processed_events
#           (pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, passenger_count, trip_distance, tip_amount, total_amount)
#           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
#        (ride.lpep_pickup_datetime, ride.lpep_dropoff_datetime,
#         ride.PULocationID, ride.DOLocationID, ride.passenger_count,
#         ride.trip_distance, ride.tip_amount, ride.total_amount)
#    )
#    count += 1
#    if count % 1000 == 0:
#        print(f"Inserted {count} rows...")
#
#consumer.close()
#cur.close()
#conn.close()

