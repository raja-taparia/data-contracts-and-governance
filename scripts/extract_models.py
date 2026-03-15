#!/usr/bin/env python3
"""
Extract dbt Model Definitions and Populate Registry

This script scans dbt/models/**/*.yml files and populates:
- models table (enriches with dbt-specific metadata)
- columns table (enhanced with dbt column definitions)
- tests table (dbt test definitions for each model/column)

Matches models to contracts via the 'meta.contract' field in the model YAML.

dbt model YAML structure:
```yaml
models:
  - name: my_model
    meta:
      contract: "domain/model_name/v1"
      owner: "Team"
      critical: true
    columns:
      - name: column_name
        tests:
          - unique
          - not_null
          - accepted_values:
              values: [a, b, c]
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


def find_model_yaml_files(models_dir: str = "dbt/models") -> List[Path]:
    """Find all dbt model definition YAML files"""
    base_path = Path(models_dir)
    if not base_path.exists():
        logger.error(f"Models directory not found: {models_dir}")
        return []
    
    files = list(base_path.glob("**/*.yml")) + list(base_path.glob("**/*.yaml"))
    logger.info(f"Found {len(files)} model definition file(s)")
    return files


def load_model_yaml(file_path: Path) -> Dict[str, Any]:
    """Load dbt model YAML file"""
    try:
        with open(file_path, 'r') as f:
            content = yaml.safe_load(f)
        return content if content else {}
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return {}


def extract_model_data(model_def: Dict[str, Any], file_path: Path) -> tuple:
    """Extract model, columns, and tests from dbt model YAML"""
    
    model_name = model_def.get('name')
    model_id = f"dbt/{model_name}" # Deterministic model_id
    
    # Model record
    meta = model_def.get('meta', {})
    config = model_def.get('config', {})
    
    model_record = {
        'id': model_id,
        'model_name': model_name,
        'schema_name': 'analytics',  # Can be enhanced from config
        'contract_id': meta.get('contract'),  # Links to contract
        'materialized_as': config.get('materialized', 'view'),
        'freshness_slo_hours': meta.get('slo_freshness_hours'),
        'completeness_slo': meta.get('slo_completeness'),
        'critical': meta.get('critical', False),
        'pii_flag': meta.get('pii_flag', False),
        'owner_name': meta.get('owner'),
    }
    
    # Column and test records
    columns_records = []
    tests_records = []
    
    columns = model_def.get('columns', [])
    for col_def in columns:
        col_name = col_def.get('name')
        col_id = f"{model_id}/{col_name}" # Deterministic col_id
        
        # Column record
        columns_records.append({
            'id': col_id,
            'model_id': model_id,
            'column_name': col_name,
            'data_type': col_def.get('data_type'),
            'required_flag': col_def.get('required', False),
            'description': col_def.get('description'),
            'enum_values_json': None,  # Can be extracted from tests
        })
        
        # Test records from column definition
        col_tests = col_def.get('tests', [])
        for test_def in col_tests:
            # Handle both string and dict test definitions
            test_name = test_def if isinstance(test_def, str) else list(test_def.keys())[0]
            test_id = f"{col_id}/{test_name}" if isinstance(test_def, str) else f"{col_id}/{test_name}/{json.dumps(test_def[test_name], sort_keys=True)}"

            tests_records.append({
                'id': test_id,
                'test_name': test_name,
                'test_type': test_name,
                'column_id': col_id,
                'model_id': model_id,
                'severity': 'error',
                'test_params': json.dumps(test_def) if isinstance(test_def, dict) else None,
                'description': f"dbt test: {test_name} on {col_name}",
            })
    
    return model_record, columns_records, tests_records


def insert_model_data(connection, config: Dict[str, Any],
                      model_record: Dict, columns: List[Dict], tests: List[Dict]) -> None:
    """Insert extracted model data into Databricks"""
    
    reg_config = config.get('registry_config', {})
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    cursor = connection.cursor()
    
    try:
        # Merge model (skip if no model name)
        if not model_record['model_name']:
            return
        
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
            USING (SELECT * FROM (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS inner_values(id, test_name, test_type, column_id, model_id, severity, test_params, description)
            ) AS src
            ON t.id = src.id
            WHEN MATCHED THEN UPDATE SET
                t.test_name = src.test_name,
                t.test_type = src.test_type,
                t.column_id = src.column_id,
                t.model_id = src.model_id,
                t.severity = src.severity,
                t.test_params = src.test_params,
                t.description = src.description
            WHEN NOT MATCHED THEN INSERT (id, test_name, test_type, column_id, model_id, severity, test_params, description, _created_ts)
                VALUES (src.id, src.test_name, src.test_type, src.column_id, src.model_id, src.severity, src.test_params, src.description, CURRENT_TIMESTAMP())
            """, (
                test['id'],
                test['test_name'],
                test['test_type'],
                test.get('column_id'),
                test['model_id'],
                test['severity'],
                test.get('test_params'),
                test['description'],
            ))
        
        logger.info(f"✓ Merged: model={model_record['model_name']}, columns={len(columns)}, tests={len(tests)}")
        
    except Exception as e:
        logger.error(f"Error inserting model data: {e}")
    finally:
        cursor.close()


def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("dbt Model Extraction and Registry Population")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*60)
    
    # Load config
    config = load_profiles()
    
    # Create connection
    connection = create_connection(config)
    
    try:
        # Find model YAMLs
        model_files = find_model_yaml_files()
        if not model_files:
            logger.warning("No model definition files found")
            return
        
        # Process each model YAML
        total_models = 0
        total_columns = 0
        total_tests = 0
        
        for file_path in model_files:
            yaml_content = load_model_yaml(file_path)
            models = yaml_content.get('models', [])
            
            for model_def in models:
                model_record, columns, tests = extract_model_data(model_def, file_path)
                insert_model_data(connection, config, model_record, columns, tests)
                
                total_models += 1
                total_columns += len(columns)
                total_tests += len(tests)
        
        logger.info("\n" + "="*60)
        logger.info("✓ MODEL EXTRACTION COMPLETED")
        logger.info("="*60)
        logger.info(f"Models: {total_models}")
        logger.info(f"Columns: {total_columns}")
        logger.info(f"Tests: {total_tests}")
        logger.info("="*60)
        
    finally:
        connection.close()


if __name__ == '__main__':
    main()
