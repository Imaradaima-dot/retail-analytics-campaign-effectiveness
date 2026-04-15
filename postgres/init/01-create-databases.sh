#!/bin/bash
# postgres/init/01-create-databases.sh
# Creates additional logical databases listed in POSTGRES_MULTIPLE_DATABASES.
# The default database (POSTGRES_DB) is already created by the official image.
# Pattern sourced from: https://github.com/mrts/docker-postgresql-multiple-databases

set -e
set -u

create_database() {
    local database=$1
    echo "  Creating database: $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        # Skip if it's the same as the default DB (already exists)
        if [ "$db" != "$POSTGRES_DB" ]; then
            create_database $db
        fi
    done
    echo "Multiple databases created."
fi
