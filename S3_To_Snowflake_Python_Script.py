
import os
import sys
import snowflake.connector as sf 
from dotenv import load_dotenv 
load_dotenv()

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")          
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE")  
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")


FILE_FORMAT_NAME_1 = os.getenv("SNOWFLAKE_FILE_FORMAT_1")
FILE_FORMAT_NAME_2 = os.getenv("SNOWFLAKE_FILE_FORMAT_2")
INTEGRATION_NAME = os.getenv("SNOWFLAKE_STORAGE_INTEGRATION")
STAGE_NAME       = os.getenv("SNOWFLAKE_STAGE_NAME")
TARGET_TABLE     = os.getenv("SNOWFLAKE_TARGET_TABLE")


S3_URL           = os.getenv("S3_URL")  
AWS_ROLE_ARN     = os.getenv("SNOWFLAKE_AWS_ROLE_ARN") 



def snow_conn():
    return sf.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )



def create_integration_phase():
    conn = snow_conn()
    c1 = conn.cursor()
   


    #Storage integration 
    c1.execute(f"""
        CREATE OR REPLACE STORAGE INTEGRATION {INTEGRATION_NAME}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'S3'
            ENABLED = TRUE
            STORAGE_AWS_ROLE_ARN = '{AWS_ROLE_ARN}'
            STORAGE_ALLOWED_LOCATIONS = ('{S3_URL}');
    """)

    #Snowflake's IAM user ARN 
    rows = c1.execute(f"DESC STORAGE INTEGRATION {INTEGRATION_NAME}").fetchall()
    
    
    dict = {}
    for r in rows:
        key = r[0]      
        value = r[2]    
        dict[key] = value
    
    
    user_arn = dict.get("STORAGE_AWS_IAM_USER_ARN", "")
    print("Snowflake IAM User ARN:", user_arn)


    c1.close()
    conn.close()

def finalize_phase():
    conn = snow_conn()
    c1 = conn.cursor()


     #File format - for header 
    c1.execute(f"""
        CREATE OR REPLACE FILE FORMAT {FILE_FORMAT_NAME_1}
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 0
        PARSE_HEADER = TRUE
    """)
    
    #File format - for data loading
    c1.execute(f"""
        CREATE OR REPLACE FILE FORMAT {FILE_FORMAT_NAME_2}
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        PARSE_HEADER = FALSE
    """)

    print("File formats ready")
    
    # Stage Creation
    c1.execute(f"""
        CREATE OR REPLACE STAGE {STAGE_NAME}
            STORAGE_INTEGRATION = {INTEGRATION_NAME}
            URL = '{S3_URL}'
            
    """)

    print(f"\nListing files at @{STAGE_NAME} ...")
    for row in c1.execute(f"LIST @{STAGE_NAME};"):
        print(row)

    print("\n Creating table using INFER_SCHEMA + TEMPLATE...")

    #Infer Schema for table creation 
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {TARGET_TABLE}
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
                LOCATION => '@{STAGE_NAME}',
                FILE_FORMAT => '{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FILE_FORMAT_NAME_1}'
            )
        )
    );
    """

    c1.execute(create_table_sql)

    print("Table created")

    #Copying the data to snowflake
    copy_sql = f"""
        COPY INTO {TARGET_TABLE}
        FROM @{STAGE_NAME}
        FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT_NAME_2})
    """
    print("\nRunning COPY INTO ...")
    for res in c1.execute(copy_sql):
        print(res)

    print("\nAll done")

    conn.close()
    c1.close()
    

if __name__ == "__main__":
    create_integration_phase()
    input("\nUpdate AWS trust policy, then press ENTER to continue\n")
    finalize_phase()