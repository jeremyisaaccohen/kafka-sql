from kafka import KafkaConsumer
from constants import TOPIC_NAME, BOOTSTRAP_SERVERS
import psycopg2

## auto_offset_reset determines if we read through whole producer if we spin up late, or if we just read messages that appear after we live
consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[BOOTSTRAP_SERVERS], auto_offset_reset='latest')

with psycopg2.connect(
    "dbname='postgres' user='postgres' host='localhost' port='5432'"
) as conn:
    conn.autocommit = True
    # Your database operations
    print("Connected to the database.")

    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
        ticker VARCHAR(100),
        time_stamp INTEGER,
        """)
        for message in consumer:
            print("new message in consumer: ", message)
            # Format the values for SQL
            try:
                cursor.execute(
                    f"INSERT INTO footy({key_string}) VALUES ({formatted_values});")
                conn.commit()
                # TODO: If inactive for a few seconds, weve exhausted consumer, print how many weve inserted.
            except:
                print(f"Error parsing {message}")