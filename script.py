import pandas as pd 
import snowflake.connector as sf 
import os 
from dotenv import load_dotenv 


load_dotenv()

df =pd.read_csv("Flight.csv")

conn = sf.connect(
    user = os.getenv("SNOWFLAKE_USER"),
    password = os.getenv("SNOWFLAKE_PASSWORD"),
    account = os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
    database = os.getenv("SNOWFLAKE_DATABASE"),
    schema = os.getenv("SNOWFLAKE_SCHEMA"),
    role = os.getenv("SNOWFLAKE_ROLE")
)

c1  = conn.cursor()

insert_query = """
insert into flight_data (Indx, airline, flight,source_city, departure_time, stops, arrival_time, destination_city, class_type,
duration, days_left, price ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


for i, record in df.iterrows():
    c1.execute(insert_query, (
        int(record['index']),
        record['airline'],
        record['flight'],
        record['source_city'],
        record['departure_time'],
        record['stops'],
        record['arrival_time'],
        record['destination_city'],
        record['class'],
        float(record['duration']),
        int(record['days_left']),
        int(record['price'])
    ))

conn.commit()


print("Data inserted successfull")

c1.close()
conn.close()
