#!/usr/bin/env python3
"""
Extract Contract Definitions and Populate Databricks Registry

This script scans the contracts/ directory for YAML files and populates:
- contracts table (contract metadata, SLOs)
- models table (model definitions linked to contracts)
- columns table (column definitions with types)
- tests table (test definitions from contract schema)

Each contract YAML should have:
```yaml
version: 1.0
name: model_name
domain: domain_name
owner:
  name: Team Name
  email: email@company.com
  slack: "#channel"
schema:
  type: object
  properties:
    column_name:
      type: string/int/...
      required: true/false
      tests: [unique, not_null, ...]
      enum: [...]
```
"""

import sys
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import yaml
from uuid import uuid4
from databricks import sql

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_profiles() -> Dict[str, Any]:
    """Load configuration from dbt/profiles.yml"""
    profiles_path = os.path.join(os.path.dirname(__file__), '..', 'dbt', 'profiles.yml')
    
    if not os.path.exists(profiles_path):
        logger.error(f"profiles.yml not found at {profiles_path}")
        sys.exit(1)
    
    with open(profiles_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Resolve environment variables
    def resolve_env_vars(value):
        if isinstance(value, str) and value.startswith("{{ env_var("):
            import re
            match = re.search(r"env_var\('([^']+)'(?:,\s*'([^']+)')?\)", value)
            if match:
                env_var = match.group(1)
                default_val = match.group(2)
                return os.getenv(env_var, default_val)
        return value
    
    def resolve_dict(d):
        if isinstance(d, dict):
            return {k: resolve_env_vars(v) if isinstance(v, str) else resolve_dict(v) 
                    for k, v in d.items()}
        elif isinstance(d, list):
            return [resolve_env_vars(v) if isinstance(v, str) else resolve_dict(v) for v in d]
        return d
    
    return resolve_dict(config)


def create_connection(config: Dict[str, Any]):
    """Create Databricks SQL connection"""
    secrets = config.get('secrets', {})
    host = secrets.get('databricks_host')
    http_path = secrets.get('databricks_http_path')
    token = secrets.get('databricks_token')
    
    if not all([host, http_path, token]):
        logger.error("Missing Databricks credentials")
        sys.exit(1)
    
    try:
        return sql.connect(server_hostname=host, http_path=http_path, access_token=token)
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)


def find_contract_files(contracts_dir: str = "contracts") -> List[Path]:
    """Find all contract YAML files"""
    base_path = Path(contracts_dir)
    if not base_path.exists():
        logger.error(f"Contracts directory not found: {contracts_dir}")
        return []
    
    files = list(base_path.glob("**/*.yaml")) + list(base_path.glob("**/*.yml"))
    logger.info(f"Found {len(files)} contract file(s)")
    return files


def load_contract(file_path: Path) -> Dict[str, Any]:
    """Load and validate contract YAML file"""
    try:
        with open(file_path, 'r') as f:
            contract = yaml.safe_load(f)
        
        # Validate required fields
        required = ['version', 'name', 'domain', 'schema']
        missing = [f for f in required if f not in contract]
        if missing:
            logger.warning(f"Contract {file_path} missing fields: {missing}")
            return None
        
        return contract
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None


def extract_contract_data(contract: Dict[str, Any], file_path: Path) -> tuple:
    """Extract contract, model, columns, and tests from contract YAML"""
    
    contract_id = f"{contract.get('domain')}/{contract.get('name')}"
    model_id = f"{contract.get('domain')}/{contract.get('name')}" # Deterministic model_id
    
    # Contract record
    owner = contract.get('owner', {})
    contract_record = {
        'id': contract_id,
        'name': contract.get('name'),
        'version': str(contract.get('version', '1.0')),
        'domain': contract.get('domain'),
        'owner_name': owner.get('name'),
        'owner_email': owner.get('email'),
        'owner_slack': owner.get('slack'),
        'source_path': str(file_path),
        'description': contract.get('description', ''),
        'slo_freshness_hours': contract.get('slo', {}).get('freshness_hours'),
        'slo_completeness': contract.get('slo', {}).get('completeness'),
        'slo_accuracy': contract.get('slo', {}).get('accuracy'),
        'slo_availability': contract.get('slo', {}).get('availability'),
        'contract_json': contract,
    }
    
    # Model record
    model_record = {
        'id': model_id,
        'model_name': contract.get('name'),
        'schema_name': 'analytics',  # Default, can be overridden
        'contract_id': contract_id,
        'materialized_as': 'table',
        'freshness_slo_hours': contract.get('slo', {}).get('freshness_hours'),
        'completeness_slo': contract.get('slo', {}).get('completeness'),
        'critical': contract.get('critical', False),
        'pii_flag': contract.get('pii_flag', False),
        'owner_name': owner.get('name'),
    }
    
    # Column and test records
    columns_records = []
    tests_records = []
    
    schema = contract.get('schema', {})
    properties = schema.get('properties', {})
    
    for col_name, col_def in properties.items():
        col_id = f"{model_id}/{col_name}" # Deterministic col_id
        
        # Column record
        columns_records.append({
            'id': col_id,
            'model_id': model_id,
            'column_name': col_name,
            'data_type': col_def.get('type'),
            'required_flag': col_def.get('required', False),
            'description': col_def.get('description'),
            'enum_values_json': json.dumps(col_def.get('enum', [])) if col_def.get('enum') else None,
        })
        
        # Test records from column definition
        col_tests = col_def.get('tests', [])
        for test_name in col_tests:
            test_id = f"{col_id}/{test_name}" # Deterministic test_id
            tests_records.append({
                'id': test_id,
                'test_name': test_name,
                'test_type': test_name,  # Simplified; could parse test params
                'column_id': col_id,
                'model_id': model_id,
                'severity': 'error',
                'description': f"Test {test_name} on {col_name}",
            })
    
    return contract_record, model_record, columns_records, tests_records


def insert_contract_data(connection, config: Dict[str, Any], 
                        contract_record: Dict, model_record: Dict, 
                        columns: List[Dict], tests: List[Dict]) -> None:
    """Insert extracted data into Databricks tables"""
    
    reg_config = config.get('registry_config', {})
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    cursor = connection.cursor()
    
    try:
        # Merge contract
        cursor.execute(f"""
        MERGE INTO {catalog}.{database}.contracts c
        USING (SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS inner_values(id, name, version, domain, owner_name, owner_email, owner_slack, source_path, description, slo_freshness_hours, slo_completeness, slo_accuracy, slo_availability, contract_json)
        ) AS src
        ON c.id = src.id AND c.version = src.version
        WHEN MATCHED THEN UPDATE SET
            c.name = src.name,
            c.domain = src.domain,
            c.owner_name = src.owner_name,
            c.owner_email = src.owner_email,
            c.owner_slack = src.owner_slack,
            c.source_path = src.source_path,
            c.description = src.description,
            c.slo_freshness_hours = src.slo_freshness_hours,
            c.slo_completeness = src.slo_completeness,
            c.slo_accuracy = src.slo_accuracy,
            c.slo_availability = src.slo_availability,
            c.contract_json = src.contract_json,
            c._updated_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (id, name, version, domain, owner_name, owner_email, owner_slack, source_path, description, slo_freshness_hours, slo_completeness, slo_accuracy, slo_availability, contract_json, _created_ts, _updated_ts, published_by)
            VALUES (src.id, src.name, src.version, src.domain, src.owner_name, src.owner_email, src.owner_slack, src.source_path, src.description, src.slo_freshness_hours, src.slo_completeness, src.slo_accuracy, src.slo_availability, src.contract_json, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_USER())
        """, (
            contract_record['id'],
            contract_record['name'],
            contract_record['version'],
            contract_record['domain'],
            contract_record['owner_name'],
            contract_record['owner_email'],
            contract_record['owner_slack'],
            contract_record['source_path'],
            contract_record['description'],
            contract_record['slo_freshness_hours'],
            contract_record['slo_completeness'],
            contract_record['slo_accuracy'],
            contract_record['slo_availability'],
            json.dumps(contract_record['contract_json']),
        ))
        
        # Merge model
        cursor.execute(f"""
        MERGE INTO {catalog}.{database}.models m
        USING (SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS inner_values(id, model_name, schema_name, contract_id, materialized_as, freshness_slo_hours, completeness_slo, critical, pii_flag, owner_name)
        ) AS src
        ON m.id = src.id
        WHEN MATCHED THEN UPDATE SET
            m.model_name = src.model_name,
            m.schema_name = src.schema_name,
            m.contract_id = src.contract_id,
            m.materialized_as = src.materialized_as,
            m.freshness_slo_hours = src.freshness_slo_hours,
            m.completeness_slo = src.completeness_slo,
            m.critical = src.critical,
            m.pii_flag = src.pii_flag,
            m.owner_name = src.owner_name,
            m._updated_ts = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (id, model_name, schema_name, contract_id, materialized_as, freshness_slo_hours, completeness_slo, critical, pii_flag, owner_name, _created_ts, _updated_ts)
            VALUES (src.id, src.model_name, src.schema_name, src.contract_id, src.materialized_as, src.freshness_slo_hours, src.completeness_slo, src.critical, src.pii_flag, src.owner_name, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """, (
            model_record['id'],
            model_record['model_name'],
            model_record['schema_name'],
            model_record['contract_id'],
            model_record['materialized_as'],
            model_record['freshness_slo_hours'],
            model_record['completeness_slo'],
            model_record['critical'],
            model_record['pii_flag'],
            model_record['owner_name'],
        ))
        
        # Merge columns
        for col in columns:
            cursor.execute(f"""
            MERGE INTO {catalog}.{database}.columns c
            USING (SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?)) AS inner_values(id, model_id, column_name, data_type, required_flag, description, enum_values_json)
            ) AS src
            ON c.id = src.id
            WHEN MATCHED THEN UPDATE SET
                c.model_id = src.model_id,
                c.column_name = src.column_name,
                c.data_type = src.data_type,
                c.required_flag = src.required_flag,
                c.description = src.description,
                c.enum_values_json = src.enum_values_json
            WHEN NOT MATCHED THEN INSERT (id, model_id, column_name, data_type, required_flag, description, enum_values_json, _created_ts)
                VALUES (src.id, src.model_id, src.column_name, src.data_type, src.required_flag, src.description, src.enum_values_json, CURRENT_TIMESTAMP())
            """, (
                col['id'],
                col['model_id'],
                col['column_name'],
                col['data_type'],
                col['required_flag'],
                col['description'],
                col['enum_values_json'],
            ))
        
        # Merge tests
        for test in tests:
            cursor.execute(f"""
            MERGE INTO {catalog}.{database}.tests t
            USING (SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?)) AS inner_values(id, test_name, test_type, column_id, model_id, severity, description)
            ) AS src
            ON t.id = src.id
            WHEN MATCHED THEN UPDATE SET
                t.test_name = src.test_name,
                t.test_type = src.test_type,
                t.column_id = src.column_id,
                t.model_id = src.model_id,
                t.severity = src.severity,
                t.description = src.description
            WHEN NOT MATCHED THEN INSERT (id, test_name, test_type, column_id, model_id, severity, description, _created_ts)
                VALUES (src.id, src.test_name, src.test_type, src.column_id, src.model_id, src.severity, src.description, CURRENT_TIMESTAMP())
            """, (
                test['id'],
                test['test_name'],
                test['test_type'],
                test.get('column_id'),
                test['model_id'],
                test['severity'],
                test['description'],
            ))
        
        logger.info(f"✓ Merged: contract={contract_record['name']}, model={model_record['model_name']}, "
                   f"columns={len(columns)}, tests={len(tests)}")
        
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        raise
    finally:
        cursor.close()


def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("Contract Extraction and Registry Population")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*60)
    
    # Load config
    config = load_profiles()
    
    # Create connection
    connection = create_connection(config)
    
    try:
        # Find contracts
        contract_files = find_contract_files()
        if not contract_files:
            logger.warning("No contract files found")
            return
        
        # Process each contract
        total_contracts = 0
        total_models = 0
        total_columns = 0
        total_tests = 0
        
        for file_path in contract_files:
            contract = load_contract(file_path)
            if not contract:
                continue
            
            contract_record, model_record, columns, tests = extract_contract_data(contract, file_path)
            insert_contract_data(connection, config, contract_record, model_record, columns, tests)
            
            total_contracts += 1
            total_models += 1
            total_columns += len(columns)
            total_tests += len(tests)
        
        logger.info("\n" + "="*60)
        logger.info("✓ CONTRACT EXTRACTION COMPLETED")
        logger.info("="*60)
        logger.info(f"Contracts: {total_contracts}")
        logger.info(f"Models: {total_models}")
        logger.info(f"Columns: {total_columns}")
        logger.info(f"Tests: {total_tests}")
        logger.info("="*60)
        
    finally:
        connection.close()


if __name__ == '__main__':
    main()
