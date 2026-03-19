# Data Contract and Governance Framework

This project provides a comprehensive framework for implementing and enforcing data contracts within a modern data stack, leveraging dbt for transformation and testing, and a relational database (like Databricks, Snowflake, etc.) for logging and governance. It allows data producers to establish clear, machine-readable agreements about the data they provide, enhancing reliability, and trust in the data platform.

---

## 🎯 Key Features

*   **Declarative Data Contracts**: Define data contracts in simple, yet powerful, YAML files. These contracts specify:
    *   **Schema**: Column names, data types, and constraints.
    *   **Service Level Objectives (SLOs)**: Guarantees for freshness, completeness, accuracy, and availability.
    *   **Ownership & Metadata**: Clear ownership, contact points, and descriptions.
    *   **Policies**: Rules for data retention and handling breaking changes.
    *   **Lineage**: Information about data sources and transformations.
*   **Automated Validation**: Python scripts orchestrate a pipeline that:
    *   Runs `dbt test` to validate schema and data quality rules defined in the contract.
    *   Logs test results to a central governance registry.
    *   Validates SLOs against the actual state of the data.
*   **Governance Registry**: A set of tables (managed by scripts like `create_registry_schema.py`) that store all governance-related information:
    *   Contract and model definitions.
    *   Historical test results.
    *   SLO validation outcomes.
    *   Data quality metrics over time.
*   **Subscriber Management**: A mechanism to track which teams or services are consuming a data asset. The framework updates a "ready for consumption" flag based on the contract validation status, allowing downstream consumers to know when data is safe to use.
*   **dbt Integration**: Seamlessly integrates with dbt:
    *   dbt models are linked to their corresponding data contracts via metadata in the model's `.yml` file.
    *   dbt tests are used to enforce many of the rules specified in the contract.
    *   The framework uses dbt's `profiles.yml` for database connection configuration.

---

## 🤔 How It Works

The framework follows a simple, yet powerful, workflow:

1.  **Define a Contract**: A data producer creates a `.yaml` file in the `contracts/` directory. This file is the single source of truth for the guarantees they are making about a data asset.

2.  **Implement the Model**: The producer implements a dbt model in the `dbt/models/` directory that builds the data asset. The model's corresponding `.yml` file links to the contract file.

3.  **Run Validation**: The `run_daily_validation.py` script executes the core validation logic:
    *   It parses the contract and model files to load their definitions into the governance registry.
    *   It runs `dbt test` to check for any violations of the defined schema and quality rules.
    *   It logs the results of these tests.
    *   It runs further validation scripts (e.g., `validate_slos.py`) to check conditions that can't be covered by dbt tests alone (like freshness).
    *   Finally, it updates the status of the data asset, marking it as ready for consumption if all checks pass.

---

## 📂 Project Structure

```
/
├── contracts/            # Directory for data contract YAML files.
├── dbt/                  # Standard dbt project.
│   ├── models/           # dbt models that implement the contracts.
│   └── dbt_project.yml   # dbt project configuration.
├── scripts/              # Python scripts for orchestration and validation.
├── run_all.py            # Master script to initialize the system.
└── README.md
```

---

## 🚀 Getting Started

### 1. Prerequisites

*   A supported data warehouse (e.g., Databricks, Snowflake).
*   Python 3.9+
*   dbt Core

### 2. Setup

1.  **Clone the repository.**
2.  **Set up a Python virtual environment and install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configure your database connection** in `dbt/profiles.yml`. This framework uses the same connection details for the dbt models and the governance registry.

### 3. Run the system

There are two main entry points for running the system:

*   **Initial Setup**: Use `run_all.py` for the first run. It creates the necessary governance tables, seeds initial data, runs the dbt models, and performs an initial validation.
    ```bash
    python run_all.py
    ```

*   **Daily Validation**: Use `scripts/run_daily_validation.py` for subsequent, scheduled runs. This script focuses on validation and does not re-run all the initial setup steps.
    ```bash
    python scripts/run_daily_validation.py
    ```

---

## ✍️ Adding Your Own Contracts

1.  **Create a new YAML file** in the `contracts/` directory (e.g., `contracts/marketing/new_data_asset/v1.yaml`). Follow the structure of the existing `v1.yaml` for accounts.
2.  **Create the corresponding dbt model** and `.yml` file in the `dbt/models/` directory.
3.  In your model's `.yml` file, add a `meta` section to link it to the contract:
    ```yaml
    models:
      - name: your_new_model
        meta:
          contract: "marketing/new_data_asset/v1"
    ```
4.  **Run `python run_all.py`** to build and validate your new asset. Subsequently, the daily validation script will automatically pick it up.
