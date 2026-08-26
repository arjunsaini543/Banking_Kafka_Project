import os
import json
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "localhost:9092"
)

# New group for testing
KAFKA_GROUP = "minio-cdc-consumer-test-v5"

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000"
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


# ============================================================
# KAFKA TOPICS
# ============================================================

TOPICS = [
    "banking_server.public.customers",
    "banking_server.public.accounts",
    "banking_server.public.transactions",
]


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    *TOPICS,

    bootstrap_servers=KAFKA_BOOTSTRAP,

    group_id=KAFKA_GROUP,

    # New consumer group reads available messages
    auto_offset_reset="earliest",

    # Commit manually after successful MinIO upload
    enable_auto_commit=False,

    # JSON
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    ),

    # Allow enough processing time
    max_poll_interval_ms=300000,
)


# ============================================================
# MINIO CLIENT
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)


# ============================================================
# CHECK MINIO
# ============================================================

try:

    s3.head_bucket(
        Bucket=MINIO_BUCKET
    )

    print(
        f"✅ MinIO bucket exists: {MINIO_BUCKET}"
    )

except Exception as e:

    print(
        f"❌ Cannot access MinIO bucket: "
        f"{MINIO_BUCKET}"
    )

    print(
        f"Error: {e}"
    )

    raise


# ============================================================
# WRITE TO MINIO
# ============================================================

def write_to_minio(table_name, record):

    if not record:
        return None

    # Convert one record to DataFrame
    df = pd.DataFrame([record])

    date_str = datetime.now().strftime(
        "%Y-%m-%d"
    )

    timestamp = datetime.now().strftime(
        "%H%M%S%f"
    )

    filename = (
        f"{table_name}_{timestamp}.parquet"
    )

    local_file = os.path.join(
        os.getcwd(),
        filename
    )

    s3_key = (
        f"{table_name}/"
        f"date={date_str}/"
        f"{filename}"
    )

    try:

        # ----------------------------------------------------
        # CREATE PARQUET
        # ----------------------------------------------------

        df.to_parquet(
            local_file,
            engine="fastparquet",
            index=False
        )

        print(
            f"📦 Created Parquet: {filename}"
        )

        # ----------------------------------------------------
        # UPLOAD TO MINIO
        # ----------------------------------------------------

        s3.upload_file(
            local_file,
            MINIO_BUCKET,
            s3_key
        )

        print(
            f"✅ Uploaded to MinIO"
        )

        print(
            f"   s3://{MINIO_BUCKET}/{s3_key}"
        )

        return s3_key

    finally:

        # ----------------------------------------------------
        # DELETE TEMP FILE
        # ----------------------------------------------------

        if os.path.exists(local_file):

            os.remove(local_file)


# ============================================================
# START
# ============================================================

print()

print("=" * 70)
print("🚀 PostgreSQL → Debezium → Kafka → MinIO")
print("=" * 70)

print(
    f"Kafka       : {KAFKA_BOOTSTRAP}"
)

print(
    f"Consumer    : {KAFKA_GROUP}"
)

print(
    f"MinIO       : {MINIO_ENDPOINT}"
)

print(
    f"Bucket      : {MINIO_BUCKET}"
)

print("=" * 70)

print(
    "🎧 Listening for PostgreSQL CDC events..."
)

print()


# ============================================================
# CONSUME KAFKA
# ============================================================

try:

    for message in consumer:

        topic = message.topic

        table_name = topic.split(".")[-1]

        print()
        print("-" * 70)

        print(
            f"Kafka topic : {topic}"
        )

        print(
            f"Partition   : {message.partition}"
        )

        print(
            f"Offset      : {message.offset}"
        )


        # ====================================================
        # GET KAFKA EVENT
        # ====================================================

        event = message.value

        if not isinstance(event, dict):

            print(
                "⚠️ Invalid Kafka event"
            )

            continue


        # ====================================================
        # IMPORTANT FIX
        #
        # Your Debezium message has:
        #
        # {
        #     "before": ...,
        #     "after": ...,
        #     "op": "u"
        # }
        #
        # NOT:
        #
        # {
        #     "payload": {
        #         "before": ...,
        #         "after": ...,
        #         "op": "u"
        #     }
        # }
        # ====================================================

        payload = event


        # ====================================================
        # GET OPERATION
        # ====================================================

        operation = payload.get("op")

        print(
            f"Operation   : {operation}"
        )


        # ====================================================
        # GET RECORD
        # ====================================================

        record = None


        # ----------------------------------------------------
        # SNAPSHOT
        # ----------------------------------------------------

        if operation == "r":

            record = payload.get("after")

            print(
                f"📸 SNAPSHOT → {table_name}"
            )


        # ----------------------------------------------------
        # INSERT
        # ----------------------------------------------------

        elif operation == "c":

            record = payload.get("after")

            print(
                f"➕ INSERT → {table_name}"
            )


        # ----------------------------------------------------
        # UPDATE
        # ----------------------------------------------------

        elif operation == "u":

            record = payload.get("after")

            print(
                f"🔄 UPDATE → {table_name}"
            )


        # ----------------------------------------------------
        # DELETE
        # ----------------------------------------------------

        elif operation == "d":

            record = payload.get("before")

            if record:

                record = {
                    **record,
                    "_operation": "DELETE"
                }

            print(
                f"🗑️ DELETE → {table_name}"
            )


        # ----------------------------------------------------
        # UNKNOWN
        # ----------------------------------------------------

        else:

            print(
                f"⚠️ Unknown operation: {operation}"
            )

            print(
                f"Event: {event}"
            )

            continue


        # ====================================================
        # CHECK RECORD
        # ====================================================

        if not record:

            print(
                "⚠️ No record found."
            )

            continue


        # ====================================================
        # SHOW RECORD
        # ====================================================

        print(
            f"📄 Record: {record}"
        )


        # ====================================================
        # UPLOAD TO MINIO
        # ====================================================

        try:

            s3_key = write_to_minio(
                table_name,
                record
            )

            # ------------------------------------------------
            # COMMIT ONLY AFTER SUCCESSFUL UPLOAD
            # ------------------------------------------------

            consumer.commit()

            print(
                "✅ Kafka offset committed"
            )

            print(
                f"✅ CDC event completed → {s3_key}"
            )

        except Exception as e:

            print(
                "❌ MinIO upload failed"
            )

            print(
                f"Error: {e}"
            )

            print(
                "⚠️ Kafka offset NOT committed"
            )

            raise


# ============================================================
# STOP
# ============================================================

except KeyboardInterrupt:

    print()
    print("=" * 70)
    print("🛑 Consumer stopped by user")
    print("=" * 70)


finally:

    consumer.close()

    print(
        "Kafka consumer closed."
    )