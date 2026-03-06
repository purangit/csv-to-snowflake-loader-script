import os
import pandas as pd
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

TABLE_NAME = "TESTING_TABLE"
TEMP_TABLE = "TESTING_TEMP"
KEY_COLUMN = "INDEX"


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


def loading():

    conn = snowflake_conn()
    c1 = conn.cursor()

    df = pd.read_csv("Flight_2.csv")

    df.columns = [col.upper().strip() for col in df.columns]

    columns_sql = ", ".join([f"{col} STRING" for col in df.columns])

    # Main Table
    c1.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        {columns_sql}
        )
    """)
    # Temp Table
    c1.execute(f"""
        CREATE OR REPLACE TEMP TABLE {TEMP_TABLE} LIKE {TABLE_NAME}
    """)

    # Loading dataframe to temp table
    write_pandas(conn, df, TEMP_TABLE)

    # columna & rows 
    cols = ",".join(df.columns) ## columns
    src_cols = ",".join([f"S.{col}" for col in df.columns]) # rows 

    merge_sql = f"""
        MERGE INTO {TABLE_NAME} T
        USING {TEMP_TABLE} S
        ON T.{KEY_COLUMN} = S.{KEY_COLUMN}
        
        WHEN MATCHED THEN   
        UPDATE SET 
        {",".join([f"T.{col}=S.{col}" for col in df.columns if col != KEY_COLUMN])}

        WHEN NOT MATCHED THEN
        INSERT ({cols})
        VALUES ({src_cols})
    """

    c1.execute(merge_sql)

    print("Incremental load completed successfully")

    c1.close()
    conn.close()


if __name__ == "__main__":
    loading()