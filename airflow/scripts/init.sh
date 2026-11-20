#!/usr/bin/env bash
set -euo pipefail

: "${AIRFLOW__CORE__FERNET_KEY:?AIRFLOW__CORE__FERNET_KEY must be set}"
: "${AIRFLOW_ADMIN_USERNAME:?AIRFLOW_ADMIN_USERNAME must be set}"
: "${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD must be set}"
: "${AIRFLOW_ADMIN_EMAIL:?AIRFLOW_ADMIN_EMAIL must be set}"

airflow db migrate

if ! airflow users list --output json | grep -Fq "\"username\": \"${AIRFLOW_ADMIN_USERNAME}\""; then
    airflow users create \
        --username "${AIRFLOW_ADMIN_USERNAME}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}" \
        --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" \
        --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
        --role Admin \
        --email "${AIRFLOW_ADMIN_EMAIL}"
fi

airflow pools set mysql_pool "${MYSQL_POOL_SLOTS:-4}" "Fleet MySQL ingestion"
airflow pools set mongo_pool "${MONGO_POOL_SLOTS:-3}" "Fleet MongoDB ingestion"
