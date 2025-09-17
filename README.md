# PostgreSQL-to-PostgreSQL Data Sync and Validation with Airflow

This project is designed to synchronize and validate data between two PostgreSQL databases using Apache Airflow. It provides a robust framework for managing data synchronization tasks, ensuring data consistency, and alerting users in case of mismatches or inconsistencies.

---

## Overview

This project automates the process of syncing data between two PostgreSQL databases. It uses Airflow to orchestrate the tasks, ensuring that data is transferred efficiently and validated for consistency. The system supports both **full table replacement** and **incremental updates** based on a timestamp or primary key.

---

## Features

- **Dynamic DAG Generation**: DAGs are dynamically created based on a JSON catalog configuration.  
- **Data Sync Methods**:  
  - `full_replace`: Replaces the entire table in the destination database.  
  - `upsert_time`: Performs incremental updates based on a timestamp column.  
- **Validation**: Ensures data consistency between source and destination tables.  
- **Alerts**: Sends Discord alerts for any data inconsistencies.  
- **Scalable**: Supports batch processing for large datasets.  
- **Customizable**: Easily configurable via a JSON catalog file.  

---

## Project Structure

```bash
├── dags/
│   ├── check_data_consistency_dag.py   # DAG for data consistency checks
│   ├── pg_to_pg_factory.py             # DAG factory for generating sync DAGs
│   ├── pg_to_pg_sync/
│   │   ├── engine.py                   # Core sync engine
│   │   ├── alerts.py                   # Discord alerting logic
│   │   ├── TableConfig.py              # Table configuration model
│   ├── DatabaseComparator.py           # Logic for comparing source and destination tables
│   ├── catalog.json                    # JSON configuration for tables to sync
├── docker-compose.yaml                 # Docker setup for Airflow
├── .env                                # Environment variables
├── .gitignore                          # Ignored files
└── README.md                           # Project documentation
````

---

## How It Works

1. **Catalog Configuration**: The `catalog.json` file defines the tables to sync, their schemas, and sync methods.
2. **Dynamic DAG Creation**: The `pg_to_pg_factory.py` script reads the catalog and generates Airflow DAGs for each table.
3. **Data Sync**: The `engine.py` handles the actual data transfer between the source and destination databases.
4. **Validation**: The `DatabaseComparator.py` ensures that the data in the source and destination tables match.
5. **Alerts**: If inconsistencies are detected, the `alerts.py` sends a notification to a Discord channel.

---

## Configuration

### 1. `catalog.json`

The `catalog.json` file defines the tables to sync. Each table configuration includes:

* `pg_schema`: Source schema name.
* `pg_table`: Source table name.
* `dst_schema`: Destination schema name.
* `dst_table`: Destination table name.
* `sync_method`: Sync method (`full_replace` or `upsert_time`).
* `primary_key`: Primary key column(s).
* `incremental_column`: Column used for incremental updates.
* `batch_size`: Number of rows to process in each batch.
* `validate`: Whether to validate the data after syncing.
* `comments`: Description of the table.

Example:

```json
{
  "pg_schema": "public",
  "pg_table": "user_data",
  "dst_schema": "public",
  "dst_table": "user_data",
  "sync_method": "upsert_time",
  "primary_key": "id",
  "incremental_column": "updated_at",
  "batch_size": 50000,
  "validate": true,
  "comments": "User data table - synced hourly"
}
```
### Docker Compose

This project requires a `docker-compose.yaml` file for running Airflow and PostgreSQL services.  
For security reasons, the actual `docker-compose.yaml` is not included in this repository.  

Instead, we provide a template:  
- Copy `docker-compose.example.yaml` to `docker-compose.yaml`.  
- Fill in the required values (database names, users, passwords, ports, etc.).  


