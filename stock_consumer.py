import json
import math
import time

from kafka import KafkaConsumer

from yahoo_finance import stock_info
from constants import STOCK_TOPIC, BOOTSTRAP_SERVERS
import psycopg2

## auto_offset_reset determines if we read through whole producer if we spin up late, or if we just read messages that appear after we live
consumer = KafkaConsumer(STOCK_TOPIC, bootstrap_servers=[BOOTSTRAP_SERVERS], auto_offset_reset='latest')

with psycopg2.connect(
    "dbname='postgres' user='postgres' host='localhost' port='5432'"
) as conn:
    conn.autocommit = True
    # Your database operations
    print("Connected to the database.")

    with conn.cursor() as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS stock_info (
        ticker VARCHAR(100),
        time_stamp FLOAT,
        currency VARCHAR(100),
        dayHigh FLOAT,
        dayLow FLOAT,
        exchange VARCHAR(100),
        fiftyDayAverage FLOAT,
        lastPrice FLOAT,
        lastVolume FLOAT,
        marketCap BIGINT,
        open FLOAT, 
        previousClose FLOAT,
        quoteType VARCHAR(100),
        regularMarketPreviousClose FLOAT,
        shares BIGINT,
        tenDayAverageVolume BIGINT,
        threeMonthAverageVolume BIGINT,
        timezone VARCHAR(100),
        twoHundredDayAverage FLOAT,
        yearChange FLOAT,
        yearHigh FLOAT,
        yearLow FLOAT
        );
        """)
        for message in consumer:
            print("new message in consumer: ", message)
            # Format the values for SQL
            try:
                timestamp = message.timestamp
                ticker = json.loads(message.value.decode())['ticker']
                keys, vals = stock_info(ticker)
                print(keys)
                print(vals)
                vals = [None if isinstance(v, float) and math.isnan(v) else v for v in vals]
                print("vals after nan", vals)
                key_string = ",".join(str(k) for k in ['ticker', 'time_stamp'] + keys)
                # Format values appropriately for SQL
                formatted_vals = []
                for v in [ticker, timestamp] + vals:
                    if v is None:
                        formatted_vals.append('NULL')
                    elif isinstance(v, str):
                        formatted_vals.append(f"'{v}'")
                    else:
                        formatted_vals.append(str(v))

                val_string = ",".join(formatted_vals)
                # val_string = "ticker,time_stamp,"+val_string
                print(key_string)
                print("val string: ", val_string)
                # val_string = f"ticker, time_stamp, {val}"
                stmt = f"INSERT INTO stock_info({key_string}) VALUES ({val_string});"
                print(f"EXECUTING:\n{stmt}")
                cursor.execute(stmt)
                conn.commit()
                print("executed")
                # TODO: If inactive for a few seconds, weve exhausted consumer, print how many weve inserted.
            except Exception as e:
                print(f"Error parsing {message}. Error received: {str(e)}")