# Dockerfile-airflow

FROM apache/airflow:3.0.0

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    boto3 \
    snowflake-connector-python \
    python-dotenv \
    dbt-core \
    dbt-snowflake