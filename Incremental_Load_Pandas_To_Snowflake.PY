import os
import pandas as pd
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "pandas_loading_1"
UNIQUE_COL = "sequence"   


def snowflake_conn():
    return sf.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )


def get_max_id(conn):
    c1 = conn.cursor()
    try:
        query = f"SELECT MAX({UNIQUE_COL}) FROM {TABLE_NAME}"
        c1.execute(query)
        result = c1.fetchone()[0]
        return result
    except:
        return None
    finally:
        c1.close()


def loading():

    conn = snowflake_conn()

    df = pd.read_csv("Flight.csv", dtype_backend="pyarrow")

    # creating stable unique column
    df["sequence"] = range(len(df))

    max_id = get_max_id(conn)

    if max_id is not None:
        df = df[df[UNIQUE_COL] > max_id]

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=TABLE_NAME,
        auto_create_table=True
    )

    print(f"Inserted Rows: {nrows},success:{success}, chunks:{nchunks}")

    conn.close()


if __name__ == "__main__":
    loading()