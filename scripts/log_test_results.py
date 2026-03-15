#!/usr/bin/env python3
"""
Log dbt Test Results to Databricks Registry

This script parses dbt/target/run_results.json (generated after 'dbt test')
and populates the test_results table with denormalized results for easy querying.

Each test result includes:
- test_id, model_name, column_name (parsed from test unique_id)
- test_name, test_type (extracted from test definition)
- status (pass/fail/skipped), failure_message, execution_time
- run_id (for grouping multiple test runs)
- run_timestamp (when the test was executed)

Denormalization allows direct SQL filtering:
  SELECT * FROM test_results 
  WHERE model_name = 'dim_accounts' 
    AND test_type = 'not_null'
    AND status = 'fail'
    AND DATE(run_timestamp) = CURRENT_DATE();

The script also enforces data retention policy (default 7 days).
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta
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


def load_run_results(run_results_file: str = "dbt/target/run_results.json") -> Dict[str, Any]:
    """Load dbt run_results.json file"""
    if not os.path.exists(run_results_file):
        logger.warning(f"Run results file not found: {run_results_file}")
        return {}
    
    try:
        with open(run_results_file, 'r') as f:
            results = json.load(f)
        logger.info(f"Loaded {len(results.get('results', []))} test results from dbt")
        return results
    except Exception as e:
        logger.error(f"Error loading run results: {e}")
        return {}


def parse_test_unique_id(unique_id: str) -> tuple:
    """Parse test unique_id to extract model and column names
    
    Examples:
    - test.data_contracts_governance.not_null_dim_accounts_account_id
    - test.data_contracts_governance.unique_dim_accounts_account_id
    - test.data_contracts_governance.accepted_values_dim_accounts_account_type__CHECKING__SAVINGS
    """
    parts = unique_id.split('.')
    if len(parts) < 3:
        return None, None
    
    test_full_name = parts[-1]
    
    # Try to extract model_name and column_name
    # Most common patterns: test_name_model_name_column_name
    
    # Split on double underscore first (for parameterized tests)
    base_name = test_full_name.split('__')[0]
    
    # List of common test prefixes
    test_prefixes = [
        'not_null_', 'unique_', 'accepted_values_', 'expression_is_true_',
        'relationships_', 'dbt_utils_', 'equal_rowcount_', 'cardinality_equality_',
        'mutually_exclusive_ranges_', 'not_accepted_values_', 'sequential_values_',
        'at_least_one_', 'recency_', 'custom_'
    ]
    
    test_name = None
    remainder = base_name
    
    for prefix in test_prefixes:
        if base_name.startswith(prefix):
            test_name = prefix.rstrip('_')
            remainder = base_name[len(prefix):]
            break
    
    # If no prefix matched, try simple split
    if not test_name:
        parts_split = base_name.split('_', 1)
        if len(parts_split) >= 2:
            test_name = parts_split[0]
            remainder = parts_split[1]
    
    # remainder should be like: model_name_column_name
    # Find where model_name ends (look for common pattern)
    tokens = remainder.split('_')
    
    # Try heuristic: first token is usually model, rest is column
    model_name = tokens[0] if tokens else None
    column_name = '_'.join(tokens[1:]) if len(tokens) > 1 else None
    
    # Override with common patterns
    # E.g., dim_accounts_account_id => model=dim_accounts, col=account_id
    for i in range(1, len(tokens)):
        potential_model = '_'.join(tokens[:i])
        potential_column = '_'.join(tokens[i:])
        
        # Heuristic: if model looks like it has plural or "dim_" prefix
        if potential_model.startswith('dim_') or potential_model.startswith('stg_'):
            model_name = potential_model
            column_name = potential_column
            break
    
    return model_name, column_name


def extract_test_results(run_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract and denormalize test results from dbt output"""
    
    test_results = []
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    
    for result in run_results.get('results', []):
        # Skip non-test results
        if result.get('resource_type') != 'test':
            continue
        
        unique_id = result.get('unique_id', '')
        status = result.get('status', 'unknown')
        message = result.get('message', '')
        execution_time_ms = int((result.get('execution_time', 0) or 0) * 1000)
        compiled_code = result.get('compiled_code', '')
        
        # Parse unique_id
        model_name, column_name = parse_test_unique_id(unique_id)
        
        # Extract test name from unique_id
        test_full_name = unique_id.split('.')[-1] if '.' in unique_id else ''
        test_name = test_full_name.split('__')[0] if test_full_name else 'unknown'
        
        # Determine test type
        test_type = 'generic'
        if 'not_null' in test_name:
            test_type = 'not_null'
        elif 'unique' in test_name:
            test_type = 'unique'
        elif 'accepted_values' in test_name:
            test_type = 'accepted_values'
        elif 'relationships' in test_name:
            test_type = 'relationships'
        elif 'expression_is_true' in test_name or 'rlike' in test_name.lower():
            test_type = 'expression_is_true'
        elif 'equal_rowcount' in test_name:
            test_type = 'equal_rowcount'
        elif 'contract' in test_name:
            test_type = 'contract_validation'
        
        # Count failures (number of rows that failed the test)
        rows_affected = None
        if status == 'fail' and result.get('failures'):
            rows_affected = result.get('failures')
        
        result_record = {
            'id': f"result_{uuid4().hex[:12]}",
            'run_id': run_id,
            'test_id': unique_id,
            'contract_id': None,  # Could be enriched by joining contracts table
            'model_name': model_name or 'unknown',
            'column_name': column_name,
            'test_name': test_name,
            'test_type': test_type,
            'status': status,
            'failure_message': message if message else None,
            'rows_affected': rows_affected,
            'execution_time_ms': execution_time_ms,
            'compiled_sql': compiled_code if compiled_code else None,
            'run_timestamp': datetime.now(),
            'configured_by': 'dbt_test',
        }
        
        test_results.append(result_record)
    
    return test_results, run_id


def insert_test_results(connection, config: Dict[str, Any],
                       test_results: List[Dict[str, Any]], run_id: str) -> None:
    """Insert test results into Databricks test_results table"""
    
    reg_config = config.get('registry_config', {})
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    if not test_results:
        logger.info("No test results to insert")
        return
    
    cursor = connection.cursor()
    
    try:
        for result in test_results:
            cursor.execute(f"""
            INSERT INTO {catalog}.{database}.test_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
            """, (
                result['id'],
                result['run_id'],
                result['test_id'],
                result['contract_id'],
                result['model_name'],
                result['column_name'],
                result['test_name'],
                result['test_type'],
                result['status'],
                result['failure_message'],
                result['rows_affected'],
                result['execution_time_ms'],
                result['compiled_sql'][:1000] if result['compiled_sql'] else None,
                result['run_timestamp'].isoformat(),
                result['configured_by'],
            ))
        
        logger.info(f"✓ Inserted {len(test_results)} test result(s) with run_id: {run_id}")
        
    except Exception as e:
        logger.error(f"Error inserting test results: {e}")
        raise
    finally:
        cursor.close()


def enforce_retention_policy(connection, config: Dict[str, Any]) -> None:
    """Delete test results older than configured retention period"""
    
    reg_config = config.get('registry_config', {})
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    retention_days = reg_config.get('retention_days', 7)
    retention_enabled = reg_config.get('retention_enabled', True)
    
    if not retention_enabled:
        logger.info("Data retention policy disabled")
        return
    
    cursor = connection.cursor()
    
    try:
        # Delete test results older than retention period
        cursor.execute(f"""
        DELETE FROM {catalog}.{database}.test_results
        WHERE run_timestamp < CURRENT_TIMESTAMP() - INTERVAL '{retention_days}' DAY
        """)
        
        deleted_rows = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
        if deleted_rows > 0:
            logger.info(f"✓ Deleted {deleted_rows} test results older than {retention_days} days")
        else:
            logger.info(f"✓ No records deleted (retention: {retention_days} days)")
        
        # Delete slo_metrics older than retention period
        cursor.execute(f"""
        DELETE FROM {catalog}.{database}.slo_metrics
        WHERE measurement_ts < CURRENT_TIMESTAMP() - INTERVAL '{retention_days}' DAY
        """)
        
        logger.info(f"✓ Data retention policy enforced ({retention_days} days)")
        
    except Exception as e:
        logger.warning(f"Could not enforce retention policy: {e}")
    finally:
        cursor.close()


def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("dbt Test Results Logging")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*60)
    
    # Load config
    config = load_profiles()
    
    # Load dbt run results
    dbt_config = config.get('dbt_config', {})
    run_results_file = dbt_config.get('test_results_file', 'dbt/target/run_results.json')
    run_results = load_run_results(run_results_file)
    
    if not run_results.get('results'):
        logger.warning("No test results found in run_results.json")
        return
    
    # Extract and denormalize results
    test_results, run_id = extract_test_results(run_results)
    
    logger.info(f"\nExtracted {len(test_results)} test result(s)")
    for result in test_results[:5]:  # Show first 5
        logger.info(f"  - {result['model_name']}.{result['column_name']}: {result['test_type']} = {result['status']}")
    if len(test_results) > 5:
        logger.info(f"  ... and {len(test_results) - 5} more")
    
    # Create Databricks connection
    connection = create_connection(config)
    
    try:
        # Insert test results
        insert_test_results(connection, config, test_results, run_id)
        
        # Enforce retention
        enforce_retention_policy(connection, config)
        
        logger.info("\n" + "="*60)
        logger.info("✓ TEST RESULT LOGGING COMPLETED")
        logger.info("="*60)
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Total results: {len(test_results)}")
        logger.info("="*60)
        
    finally:
        connection.close()


if __name__ == '__main__':
    main()
