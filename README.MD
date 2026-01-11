# <ins> KitchenOps Inventory Tracker </ins>

A **Dockerized ETL pipeline** for tracking restaurant kitchen inventory over time using PostgreSQL.
The system ingests raw CSV files, validates data, loads structured tables, and enables historical inventory analysis such as usage trends and stock changes.

Project Overview

This project simulates how a restaurant kitchen manages inventory data in a production environment.

## Core goals:

- Ingest raw inventory data into an inbox for processing

- Validate data and reject invalid records with clear error messaging

- Track inventory levels over defined time periods

- Enforce data quality through row-level validation and database constraints

- Enable analytics such as inventory drops, restocks, and usage trends

## 🚀 How to Run

This project runs locally using Docker. No prior database or Python setup is required.

1. Clone the Repo

    ``` 
    https://github.com/micheal-egg/Restaurant-Tracker
    cd Restaurant-Tracker
    ```

2. Create an .env file at your project root:

    ```
    DB_USER=your_user
    DB_PASSWORD=your_password
    DB_HOST=db
    DB_PORT=5432
    DB_NAME=kitchenops

    PGADMIN_DEFAULT_EMAIL=admin@example.com
    PGADMIN_DEFAULT_PASSWORD=your_password 
    ```


3. Start the Infrastructure 
    ```
    docker compose up -d 
    ```
    - This will start up Postgre SQL

4. You can place sample input data into "data/inbox/" (Included in my repo)

5. Run ETL Pipeline 
    ```
    docker compose run --rm etl 
    ```
    - This will read the CSVs in inbox, validate the rows and load clean data to PostgreSQL, then move data to processed or rejected

6. Stopping the Project 
    ```
    `docker compose down -v `
    ```
    

**What This Project Demonstrates**

Dockerized infrastructure and service orchestration

ETL pipeline design with validation and error handling

Relational data modeling (dimensions + fact tables)

Historical inventory tracking and analytics

Production-style file lifecycle management

This project can be fully run locally in under 5 minutes using Docker and demonstrates production-style data ingestion and analytics patterns.

