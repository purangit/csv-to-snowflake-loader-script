import os 
import pandas as pd 
import snowflake.connector as sf 
from snowflake.connector.pandas_tools import write_pandas 
from dotenv import load_dotenv
load_dotenv()

TABLE_NAME = "loading_data_with_pandas"
OVERWRITE  = True

def snowflake_conn():
    return sf.connect(
    user = os.getenv("SNOWFLAKE_USER"),
    password = os.getenv("SNOWFLAKE_PASSWORD"),
    account = os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
    database = os.getenv("SNOWFLAKE_DATABASE"),
    schema = os.getenv("SNOWFLAKE_SCHEMA"),
    role = os.getenv("SNOWFLAKE_ROLE")
    )


def loading():
    conn = snowflake_conn()
    df = pd.read_csv("Flight.csv", dtype_backend = "pyarrow")

    success,nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name = TABLE_NAME,
        auto_create_table = True,
        overwrite = OVERWRITE
    )
    print(f"success:{success}, Number of rows:{nrows}, No of chunks: {nchunks}")

    conn.close()


if __name__ == "__main__":
    loading()