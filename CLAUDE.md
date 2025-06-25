# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project is a Python-based data pipeline for fetching stock market data from Yahoo Finance (`yfinance`) and storing it in a PostgreSQL database. It handles fetching security information, historical daily prices, and corporate actions like dividends and splits.

## Codebase Architecture

The project is structured into several key components:

-   `main.py`: The main entry point for the application. It orchestrates the data fetching and database storage process for a predefined list of stock symbols.
-   `data_updater.py`: Contains the core logic for fetching data from the `yfinance` API. It fetches basic security information and historical price data, and includes functions to calculate price adjustment factors.
-   `db_manager.py`: A database management layer that uses SQLAlchemy to interact with the PostgreSQL database. It handles session management, and provides methods for bulk inserting/updating data (`bulk_upsert`).
-   `data_models/models.py`: Defines the database schema using SQLAlchemy ORM. This includes tables for securities, daily prices, and corporate actions.
-   `alembic/`: Manages database schema migrations using Alembic. The configuration is in `alembic.ini`.
-   `requirements.txt`: Lists all the Python dependencies for the project.

## Development Setup

1.  **Environment Variables**: Create a `.env` file in the root directory to store the database connection string.

    ```bash
    # .env
    DATABASE_URL="postgresql://user:password@host:port/database"
    ```

2.  **Install Dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

## Common Commands

-   **Run the data fetching process**:
    ```bash
    python main.py
    ```
    You can modify the `symbols_to_process` list in `main.py` to fetch data for different stocks.

-   **Database Migrations (Alembic)**:
    -   To automatically generate a new migration script based on changes in `data_models/models.py`:
        ```bash
        alembic revision --autogenerate -m "Your migration description"
        ```
    -   To apply the latest migrations to the database:
        ```bash
        alembic upgrade head
        ```
