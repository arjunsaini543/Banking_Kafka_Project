import requests
import json
import time

CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "postgres-connector"

config = {
    "name": CONNECTOR_NAME,

    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # PostgreSQL connection
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "arjun2003",
        "database.dbname": "Banking",

        # Debezium
        "topic.prefix": "banking_server",
        "plugin.name": "pgoutput",

        # IMPORTANT:
        # First capture existing PostgreSQL rows
        "snapshot.mode": "initial",

        # Publication
        "publication.name": "dbz_publication",
        "publication.autocreate.mode": "all_tables",

        # Replication slot
        "slot.name": "banking_server_slot",

        # Tables to monitor
        "table.include.list":
            "public.customers,"
            "public.accounts,"
            "public.transactions",

        # JSON without Kafka Connect schema
        "key.converter":
            "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",

        "value.converter":
            "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
    }
}


# ------------------------------------------------
# Check whether connector already exists
# ------------------------------------------------

response = requests.get(
    f"{CONNECT_URL}/{CONNECTOR_NAME}"
)

if response.status_code == 200:

    print("ℹ️ Connector already exists.")

    # Update existing connector
    response = requests.put(
        f"{CONNECT_URL}/{CONNECTOR_NAME}/config",
        json=config["config"]
    )

    if response.status_code in [200, 201]:
        print("✅ Connector configuration updated.")
    else:
        print("❌ Failed to update connector.")
        print(response.text)

else:

    # Create connector
    response = requests.post(
        CONNECT_URL,
        json=config
    )

    if response.status_code in [200, 201]:
        print("✅ Connector created successfully!")
    else:
        print("❌ Failed to create connector.")
        print(response.text)


# ------------------------------------------------
# Wait and check status
# ------------------------------------------------

time.sleep(3)

status = requests.get(
    f"{CONNECT_URL}/{CONNECTOR_NAME}/status"
)

print("\nConnector status:")
print(json.dumps(status.json(), indent=2))