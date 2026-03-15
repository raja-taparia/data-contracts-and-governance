"""Publish a data contract into a Databricks SQL contract registry table.

This script is intended to store contract YAML content, metadata, and publication
timestamps into a Databricks SQL table so that contract versions and consumers
can be tracked from within Databricks.

Usage:
    python scripts/publish_contract_databricks.py \
      --host <workspace-host> \
      --http_path <sql-warehouse-http-path> \
      --token <pat> \
      --catalog <catalog> \
      --schema <schema> \
      --contract contracts/finance/accounts/v1.yaml \
      --publisher "Finance Data Team"

Requirements:
  pip install databricks-sql-connector PyYAML
"""

import argparse
import json
import yaml
from datetime import datetime

from databricks import sql


def load_contract(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_registry_table(cursor, catalog: str, schema: str, table: str):
    full_name = f"{catalog}.{schema}.{table}"

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            contract_id STRING NOT NULL,
            contract_name STRING NOT NULL,
            version STRING NOT NULL,
            domain STRING,
            contract_content STRING NOT NULL,
            published_at TIMESTAMP NOT NULL,
            publisher STRING NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (contract_id, version)
        )
        USING DELTA
        """
    )

    print(f"Ensured registry table exists: {full_name}")


def publish_contract(
    cursor,
    catalog: str,
    schema: str,
    table: str,
    contract: dict,
    publisher: str,
):
    full_name = f"{catalog}.{schema}.{table}"

    contract_id = f"{contract.get('domain')}_{contract.get('name')}".lower()
    published_at = datetime.utcnow().isoformat() + "Z"

    cursor.execute(
        f"""
        INSERT INTO {full_name} (
            contract_id,
            contract_name,
            version,
            domain,
            contract_content,
            published_at,
            publisher
        ) VALUES (%(contract_id)s, %(contract_name)s, %(version)s, %(domain)s, %(contract_content)s, %(published_at)s, %(publisher)s)
        """,
        {
            "contract_id": contract_id,
            "contract_name": contract.get("name"),
            "version": str(contract.get("version")),
            "domain": contract.get("domain"),
            "contract_content": json.dumps(contract),
            "published_at": published_at,
            "publisher": publisher,
        },
    )

    print(f"Published contract {contract_id} v{contract.get('version')} to {full_name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish a contract to Databricks SQL")
    parser.add_argument("--host", required=True, help="Databricks workspace host")
    parser.add_argument("--http_path", required=True, help="Databricks SQL Warehouse HTTP path")
    parser.add_argument("--token", required=True, help="Databricks personal access token")
    parser.add_argument("--catalog", required=True, help="Databricks catalog name")
    parser.add_argument("--schema", required=True, help="Databricks schema name")
    parser.add_argument(
        "--table",
        default="contract_registry",
        help="Table name to store contracts (default: contract_registry)",
    )
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--publisher", required=True, help="Publisher name/identifier")

    args = parser.parse_args()

    contract = load_contract(args.contract)

    conn = sql.connect(
        server_hostname=args.host,
        http_path=args.http_path,
        personal_access_token=args.token,
    )

    try:
        cursor = conn.cursor()
        ensure_registry_table(cursor, args.catalog, args.schema, args.table)
        publish_contract(
            cursor,
            args.catalog,
            args.schema,
            args.table,
            contract,
            args.publisher,
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
