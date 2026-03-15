# Data Contracts & Governance System (dbt + Databricks)

A complete **data governance platform** for managing data contracts, running tests, validating SLOs, and tracking data quality—all with dbt and Databricks.

---

## 🎯 Key Functionalities

*   **Data Contracts**: YAML-based contract definitions with schema, SLOs, and ownership.
*   **Test Execution & Logging**: Automated dbt test runs with results stored in Databricks.
*   **SLO Validation**: Validate freshness, completeness, accuracy, and availability.
*   **Data Quality Monitoring**: Detect schema drift, null spikes, and other anomalies.
*   **Subscriber Management**: Notify subscribers of data readiness based on contract status.
*   **Centralized Configuration**: All settings managed in a single `dbt/profiles.yml` file.

---

## 🚀 Quickstart

### 1. Prerequisites

*   A Databricks account with a SQL Warehouse.
*   A Personal Access Token (PAT) for Databricks.
*   Python 3.9+ and Git installed locally.

### 2. Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd data-contracts-and-governance
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Databricks connection:**
    Edit `dbt/profiles.yml` and add your Databricks credentials to the `secrets` section. It is recommended to use environment variables for production use.

### 3. Run the system

There are two main scripts to run the system:

*   **`run_all.py`**: Use this for the initial setup. It creates the database schema, seeds the tables, and populates the contract and model registries.
    ```bash
    python3 run_all.py
    ```

*   **`scripts/run_daily_validation.py`**: Run this script daily to perform ongoing validation of your data.
    ```bash
    python3 scripts/run_daily_validation.py
    ```

---

## ✅ Recent Fixes and Improvements

This project has been recently updated to fix several issues and improve stability. The following changes have been made:

*   **Corrected SQL MERGE Statements**: Fixed several "unresolved column" errors in `scripts/extract_contracts.py` and `scripts/extract_models.py` by aligning the SQL queries with the database schema.
*   **Fixed Failing dbt Test**: Resolved a data validation error in the `dim_accounts` model by correcting invalid data in the `dbt/seeds/accounts.csv` seed file.
*   **Corrected dbt Command Execution**: Fixed an issue in `run_all.py` where the `dbt seed` command was being run from the wrong directory.
*   **Environment Stability**: Addressed Python package compatibility issues that were causing `dbt` commands to fail.

The `run_all.py` and `scripts/run_daily_validation.py` scripts now run successfully.

---

## ⚙️ Configuration Guide

All system settings are centralized in `dbt/profiles.yml`. Refer to the comments in the file for details on how to configure the registry, validation rules, and notifications.

---

## 🐛 Troubleshooting

If you encounter any issues, please ensure that:
1.  Your Databricks credentials in `dbt/profiles.yml` are correct.
2.  You are using a Python virtual environment with the dependencies from `requirements.txt` installed.
3.  You have run the `run_all.py` script at least once to set up the database schema.
