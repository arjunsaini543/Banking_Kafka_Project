import os
from datetime import timedelta

import boto3
import pendulum
import snowflake.connector

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()


# ============================================================
# MINIO CONFIG
# ============================================================

# IMPORTANT:
# Airflow runs inside Docker, so use the Docker service name.
MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT_AIRFLOW",
    "http://minio:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin123"
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "banking-cdc"
)

LOCAL_DIR = os.getenv(
    "MINIO_LOCAL_DIR",
    "/tmp/minio_downloads"
)


# ============================================================
# SNOWFLAKE CONFIG
# ============================================================

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

SNOWFLAKE_DB = os.getenv(
    "SNOWFLAKE_DB",
    "BANKING"
)

SNOWFLAKE_SCHEMA = os.getenv(
    "SNOWFLAKE_SCHEMA",
    "RAW"
)


# ============================================================
# SNOWFLAKE OBJECTS
# ============================================================

SNOWFLAKE_STAGE = "BANKING.RAW.BANKING_STAGE"

SNOWFLAKE_TABLES = {
    "customers": "BANKING.RAW.CUSTOMERS",
    "accounts": "BANKING.RAW.ACCOUNTS",
    "transactions": "BANKING.RAW.TRANSACTIONS",
}


# ============================================================
# MINIO TABLES
# ============================================================

TABLES = [
    "customers",
    "accounts",
    "transactions",
]


# ============================================================
# AUDIT TABLE
# ============================================================

AUDIT_TABLE = "BANKING.RAW.FILE_LOAD_AUDIT"


# ============================================================
# MINIO CLIENT
# ============================================================

def get_minio_client():

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


# ============================================================
# SNOWFLAKE CONNECTION
# ============================================================

def get_snowflake_connection():

    required = {
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    }

    missing = [
        key
        for key, value in required.items()
        if not value
    ]

    if missing:
        raise ValueError(
            "Missing Snowflake environment variables: "
            + ", ".join(missing)
        )

    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )


# ============================================================
# CREATE AUDIT TABLE
# ============================================================

def create_audit_table():

    print("========================================")
    print("CREATING / CHECKING AUDIT TABLE")
    print("========================================")

    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                MINIO_KEY VARCHAR NOT NULL,
                TABLE_NAME VARCHAR NOT NULL,
                FILE_NAME VARCHAR NOT NULL,
                LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                STATUS VARCHAR DEFAULT 'SUCCESS'
            )
        """)

        conn.commit()

        print(
            f"✅ Audit table ready: {AUDIT_TABLE}"
        )

    finally:

        cur.close()
        conn.close()


# ============================================================
# GET SUCCESSFULLY PROCESSED FILES
# ============================================================

def get_processed_files():

    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:

        cur.execute(f"""
            SELECT MINIO_KEY
            FROM {AUDIT_TABLE}
            WHERE STATUS = 'SUCCESS'
        """)

        rows = cur.fetchall()

        return {
            row[0]
            for row in rows
        }

    finally:

        cur.close()
        conn.close()


# ============================================================
# TASK 1
# DOWNLOAD NEW FILES FROM MINIO
# ============================================================

def download_from_minio():

    print("========================================")
    print("STARTING MINIO DOWNLOAD")
    print("========================================")

    print(f"MinIO endpoint : {MINIO_ENDPOINT}")
    print(f"MinIO bucket   : {MINIO_BUCKET}")
    print(f"Local directory: {LOCAL_DIR}")

    os.makedirs(
        LOCAL_DIR,
        exist_ok=True
    )

    create_audit_table()

    s3 = get_minio_client()

    print("----------------------------------------")
    print("Testing MinIO connection")
    print("----------------------------------------")

    s3.head_bucket(
        Bucket=MINIO_BUCKET
    )

    print(
        f"✅ Connected to bucket: {MINIO_BUCKET}"
    )

    processed_files = get_processed_files()

    print(
        f"Previously processed files: "
        f"{len(processed_files)}"
    )

    local_files = {
        table: []
        for table in TABLES
    }

    for table in TABLES:

        print()
        print("========================================")
        print(f"PROCESSING: {table}")
        print("========================================")

        prefix = f"{table}/"

        paginator = s3.get_paginator(
            "list_objects_v2"
        )

        new_files_count = 0

        for page in paginator.paginate(
            Bucket=MINIO_BUCKET,
            Prefix=prefix
        ):

            objects = page.get(
                "Contents",
                []
            )

            for obj in objects:

                key = obj["Key"]

                if key.endswith("/"):
                    continue

                if not key.lower().endswith(".parquet"):
                    continue

                if key in processed_files:

                    print(
                        f"⏭️ Already processed: {key}"
                    )

                    continue

                filename = os.path.basename(key)

                table_dir = os.path.join(
                    LOCAL_DIR,
                    table
                )

                os.makedirs(
                    table_dir,
                    exist_ok=True
                )

                local_file = os.path.join(
                    table_dir,
                    filename
                )

                print("----------------------------------------")
                print("NEW FILE")
                print(f"MinIO : {key}")
                print(f"Local : {local_file}")
                print("----------------------------------------")

                s3.download_file(
                    MINIO_BUCKET,
                    key,
                    local_file
                )

                if not os.path.isfile(
                    local_file
                ):

                    raise FileNotFoundError(
                        f"Download failed: {local_file}"
                    )

                print(
                    f"✅ Downloaded: {filename}"
                )

                local_files[table].append({
                    "local_file": local_file,
                    "minio_key": key,
                    "filename": filename,
                })

                new_files_count += 1

        print(
            f"{table}: "
            f"{new_files_count} new file(s)"
        )

    total_files = sum(
        len(files)
        for files in local_files.values()
    )

    print()
    print("========================================")
    print("MINIO DOWNLOAD COMPLETED")
    print("========================================")

    print(
        f"Total new files downloaded: "
        f"{total_files}"
    )

    return local_files


# ============================================================
# TASK 2
# LOAD FILES INTO SNOWFLAKE
# ============================================================

def load_to_snowflake(**kwargs):

    print("========================================")
    print("STARTING SNOWFLAKE LOAD")
    print("========================================")

    # --------------------------------------------------------
    # GET XCOM
    # --------------------------------------------------------

    ti = kwargs["ti"]

    local_files = ti.xcom_pull(
        task_ids="download_minio"
    )

    if not local_files:

        print(
            "No files received from MinIO task."
        )

        return

    total_files = sum(
        len(files)
        for files in local_files.values()
    )

    if total_files == 0:

        print(
            "No new files to load."
        )

        return

    print(
        f"Files received: {total_files}"
    )

    # --------------------------------------------------------
    # CREATE AUDIT TABLE
    # --------------------------------------------------------

    create_audit_table()

    # --------------------------------------------------------
    # CONNECT TO SNOWFLAKE
    # --------------------------------------------------------

    print("----------------------------------------")
    print("Connecting to Snowflake")
    print("----------------------------------------")

    conn = get_snowflake_connection()

    cur = conn.cursor()

    try:

        # ====================================================
        # VERIFY CONNECTION
        # ====================================================

        cur.execute("""
            SELECT
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_WAREHOUSE()
        """)

        result = cur.fetchone()

        print(
            f"Database  : {result[0]}"
        )

        print(
            f"Schema    : {result[1]}"
        )

        print(
            f"Warehouse : {result[2]}"
        )

        # ====================================================
        # CHECK STAGE
        # ====================================================

        print("----------------------------------------")
        print("Checking Snowflake stage")
        print("----------------------------------------")

        cur.execute("""
            SHOW STAGES LIKE 'BANKING_STAGE'
            IN SCHEMA BANKING.RAW
        """)

        stage_result = cur.fetchall()

        if not stage_result:

            raise RuntimeError(
                "BANKING.RAW.BANKING_STAGE "
                "does not exist."
            )

        print(
            "✅ BANKING.RAW.BANKING_STAGE exists."
        )

        # ====================================================
        # PROCESS TABLES
        # ====================================================

        for table, files in local_files.items():

            if not files:
                continue

            target_table = SNOWFLAKE_TABLES.get(
                table
            )

            if not target_table:

                raise ValueError(
                    f"No Snowflake table mapping "
                    f"for {table}"
                )

            # =================================================
            # IMPORTANT FIX
            #
            # DO NOT USE:
            #
            # @BANKING_STAGE/accounts
            #
            # Use the stage ROOT.
            # =================================================

            stage_path = (
                f"@{SNOWFLAKE_STAGE}"
            )

            print()
            print("========================================")
            print(
                f"TARGET: {target_table}"
            )
            print("========================================")

            for file_info in files:

                file_path = file_info[
                    "local_file"
                ]

                minio_key = file_info[
                    "minio_key"
                ]

                filename = file_info[
                    "filename"
                ]

                if not os.path.isfile(
                    file_path
                ):

                    raise FileNotFoundError(
                        f"Local file missing: "
                        f"{file_path}"
                    )

                print()
                print("----------------------------------------")
                print("PROCESSING FILE")
                print("----------------------------------------")

                print(
                    f"MinIO key : {minio_key}"
                )

                print(
                    f"Filename  : {filename}"
                )

                # =================================================
                # PUT FILE TO STAGE ROOT
                # =================================================

                file_uri = (
                    "file://"
                    + os.path.abspath(file_path)
                )

                put_sql = f"""
PUT '{file_uri}'
{stage_path}
AUTO_COMPRESS = FALSE
OVERWRITE = TRUE
"""

                print(
                    "Uploading to Snowflake stage..."
                )

                cur.execute(
                    put_sql
                )

                put_results = cur.fetchall()

                for row in put_results:

                    print(
                        "PUT result:",
                        row
                    )

                print(
                    "✅ PUT completed."
                )

                # =================================================
                # VERIFY FILE EXISTS IN STAGE
                # =================================================

                print(
                    "Checking uploaded file in stage..."
                )

                cur.execute(
                    f"""
                    LIST {stage_path}
                    """
                )

                stage_files = cur.fetchall()

                stage_file_found = False

                for row in stage_files:

                    stage_name = str(
                        row[0]
                    )

                    print(
                        f"Stage file: {stage_name}"
                    )

                    if stage_name.endswith(
                        filename
                    ):

                        stage_file_found = True

                if not stage_file_found:

                    raise RuntimeError(
                        f"File was uploaded but "
                        f"could not be found in "
                        f"stage: {filename}"
                    )

                print(
                    f"✅ File exists in stage: "
                    f"{filename}"
                )

                # =================================================
                # COPY EXACT FILE
                # =================================================

                print("----------------------------------------")
                print(
                    f"Copying {filename} "
                    f"into {target_table}"
                )
                print("----------------------------------------")

                copy_sql = f"""
COPY INTO {target_table}
FROM {stage_path}
FILES = ('{filename}')
FILE_FORMAT = (
    TYPE = PARQUET
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT'
"""

                print(
                    "COPY SQL:"
                )

                print(
                    copy_sql
                )

                cur.execute(
                    copy_sql
                )

                copy_results = cur.fetchall()

                print(
                    "COPY result:"
                )

                for row in copy_results:

                    print(row)

                # =================================================
                # VALIDATE COPY
                # =================================================

                loaded = False

                for row in copy_results:

                    if len(row) >= 4:

                        status = str(
                            row[1]
                        ).upper()

                        rows_loaded = row[3]

                        print(
                            f"Status      : "
                            f"{status}"
                        )

                        print(
                            f"Rows loaded : "
                            f"{rows_loaded}"
                        )

                        if status in (
                            "LOADED",
                            "PARTIALLY_LOADED"
                        ):

                            loaded = True

                if not loaded:

                    raise RuntimeError(
                        f"Snowflake COPY failed "
                        f"for {filename}. "
                        f"Result: {copy_results}"
                    )

                # =================================================
                # AUDIT
                # =================================================

                cur.execute(
                    f"""
                    INSERT INTO {AUDIT_TABLE}
                    (
                        MINIO_KEY,
                        TABLE_NAME,
                        FILE_NAME,
                        LOADED_AT,
                        STATUS
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        CURRENT_TIMESTAMP(),
                        'SUCCESS'
                    )
                    """,
                    (
                        minio_key,
                        table,
                        filename,
                    )
                )

                # =================================================
                # COMMIT
                # =================================================

                conn.commit()

                print(
                    f"✅ Successfully loaded: "
                    f"{minio_key}"
                )

                # =================================================
                # DELETE LOCAL TEMP FILE
                # =================================================

                if os.path.exists(
                    file_path
                ):

                    os.remove(
                        file_path
                    )

                    print(
                        f"🗑️ Deleted local file: "
                        f"{filename}"
                    )

        print()
        print("========================================")
        print(
            "SNOWFLAKE LOAD COMPLETED SUCCESSFULLY"
        )
        print("========================================")

    except Exception:

        conn.rollback()

        print(
            "❌ Snowflake load failed."
        )

        raise

    finally:

        cur.close()
        conn.close()


# ============================================================
# AIRFLOW DEFAULT ARGUMENTS
# ============================================================

default_args = {

    "owner": "airflow",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(
        minutes=1
    ),
}


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(

    dag_id="minio_to_snowflake_banking",

    default_args=default_args,

    description=(
        "Incrementally load Banking "
        "Parquet files from MinIO "
        "into Snowflake RAW"
    ),

    schedule="0 2 * * *",

    start_date=pendulum.datetime(
        2026,
        8,
        2,
        tz="Asia/Kolkata"
    ),

    catchup=False,

    tags=[
        "banking",
        "minio",
        "snowflake",
        "incremental",
    ],

) as dag:

    download_task = PythonOperator(

        task_id="download_minio",

        python_callable=download_from_minio,

    )

    snowflake_task = PythonOperator(

        task_id="load_snowflake",

        python_callable=load_to_snowflake,

    )

    download_task >> snowflake_task