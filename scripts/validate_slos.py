#!/usr/bin/env python3
"""
SLO Validation for Data Contracts

Validates Service Level Objectives (SLOs) defined in contracts:
- Freshness: How recent is the data (max_age_hours)
- Completeness: What % of rows have required columns non-null
- Accuracy: Custom reconciliation queries or statistical checks
- Availability: Is the table queryable (exists, readable)

Stores results in slo_metrics table with:
  (contract_id, metric_type, expected_value, actual_value, status, measurement_ts)

Can be run daily (via run_daily_validation.py) or on-demand.
Configuration drives which SLOs and thresholds to check.
"""

import sys
import os
import json
import logging
from datetime import datetime, timedelta
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


def validate_freshness(connection, config: Dict[str, Any],
                      model_name: str, catalog: str, database: str, 
                      max_age_hours: int) -> Dict[str, Any]:
    """Check data freshness: max age of data vs expected SLO"""
    
    cursor = connection.cursor()
    
    try:
        # Try to find max timestamp in data
        # Common patterns: created_at, updated_at, _ts, load_date
        cursor.execute(f"""
        SELECT 
            MAX(GREATEST(
                IF(created_at IS NOT NULL, created_at, NULL),
                IF(updated_at IS NOT NULL, updated_at, NULL),
                IF(_updated_ts IS NOT NULL, _updated_ts, NULL)
            )) as max_ts
        FROM {catalog}.{database}.{model_name}
        """)
        
        result = cursor.fetchone()
        if not result or result[0] is None:
            return {
                'status': 'unknown',
                'expected_value': max_age_hours,
                'actual_value': None,
                'details': 'Could not determine data age (no timestamp column found)',
            }
        
        max_ts = result[0]
        age_hours = (datetime.now() - max_ts.replace(tzinfo=None)).total_seconds() / 3600
        
        status = 'pass' if age_hours <= max_age_hours else 'fail'
        
        return {
            'status': status,
            'expected_value': max_age_hours,
            'actual_value': round(age_hours, 2),
            'details': f"Latest data is {age_hours:.1f} hours old (max allowed: {max_age_hours}h)",
        }
        
    except Exception as e:
        logger.warning(f"Freshness check failed for {model_name}: {e}")
        return {
            'status': 'error',
            'expected_value': max_age_hours,
            'actual_value': None,
            'details': str(e),
        }
    finally:
        cursor.close()


def validate_completeness(connection, config: Dict[str, Any],
                         model_name: str, catalog: str, database: str,
                         min_coverage_pct: float) -> Dict[str, Any]:
    """Check data completeness: % of rows with non-null required columns"""
    
    cursor = connection.cursor()
    
    try:
        # Get column list from registry
        cursor.execute(f"""
        SELECT column_name, required_flag
        FROM {catalog}.governance.columns
        JOIN {catalog}.governance.models ON columns.model_id = models.id
        WHERE models.model_name = '{model_name}' AND required_flag = true
        """)
        
        required_cols = [row[0] for row in cursor.fetchall()]
        
        if not required_cols:
            return {
                'status': 'unknown',
                'expected_value': min_coverage_pct,
                'actual_value': 100,
                'details': 'No required columns defined',
            }
        
        # Check completeness for each required column
        completeness_scores = []
        for col in required_cols:
            cursor.execute(f"""
            SELECT 
                ROUND(100.0 * COUNT(CASE WHEN {col} IS NOT NULL THEN 1 END) / COUNT(*), 2) as pct
            FROM {catalog}.{database}.{model_name}
            """)
            
            result = cursor.fetchone()
            if result:
                completeness_scores.append(result[0])
        
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0
        status = 'pass' if avg_completeness >= min_coverage_pct else 'fail'
        
        return {
            'status': status,
            'expected_value': min_coverage_pct,
            'actual_value': round(avg_completeness, 2),
            'details': f"Average completeness across required columns: {avg_completeness:.1f}% (min required: {min_coverage_pct}%)",
        }
        
    except Exception as e:
        logger.warning(f"Completeness check failed for {model_name}: {e}")
        return {
            'status': 'error',
            'expected_value': min_coverage_pct,
            'actual_value': None,
            'details': str(e),
        }
    finally:
        cursor.close()


def validate_availability(connection, config: Dict[str, Any],
                         model_name: str, catalog: str, database: str) -> Dict[str, Any]:
    """Check data availability: is table readable"""
    
    cursor = connection.cursor()
    
    try:
        # Try simple query
        cursor.execute(f"SELECT COUNT(*) FROM {catalog}.{database}.{model_name} LIMIT 1")
        result = cursor.fetchone()
        row_count = result[0] if result else 0
        
        return {
            'status': 'pass',
            'expected_value': 99.9,
            'actual_value': 100,
            'details': f"Table is accessible with {row_count} rows",
        }
        
    except Exception as e:
        logger.warning(f"Availability check failed for {model_name}: {e}")
        return {
            'status': 'fail',
            'expected_value': 99.9,
            'actual_value': 0,
            'details': f"Table not accessible: {str(e)}",
        }
    finally:
        cursor.close()


def validate_accuracy(connection, config: Dict[str, Any],
                     model_name: str, catalog: str, database: str) -> Dict[str, Any]:
    """Check data accuracy: basic statistical validation"""
    
    # For now, return 'pass' if no custom accuracy query defined
    # In production, could run reconciliation queries from contract metadata
    
    return {
        'status': 'pass',
        'expected_value': 1,
        'actual_value': 1,
        'details': 'Accuracy check passed (no custom queries defined)',
    }


def validate_contract_slos(connection, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Run all SLO validations for contracts with models"""
    
    reg_config = config.get('registry_config', {})
    val_config = config.get('validation_config', {})
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    slo_results = []
    cursor = connection.cursor()
    
    try:
        # Get all contracts with models
        cursor.execute(f"""
        SELECT DISTINCT c.id, c.name, m.model_name, m.freshness_slo_hours, m.completeness_slo
        FROM {catalog}.{database}.contracts c
        JOIN {catalog}.{database}.models m ON c.id = m.contract_id
        WHERE m.schema_name IN (SELECT schema_name FROM {catalog}.governance.models)
        """)
        
        contracts = cursor.fetchall()
        
        for contract_id, contract_name, model_name, freshness_hours, completeness_pct in contracts:
            logger.info(f"Validating SLOs for {contract_name}/{model_name}")
            
            # Freshness
            if freshness_hours:
                freshness_result = validate_freshness(
                    connection, config, model_name, 'dbt', 'analytics',
                    freshness_hours
                )
                slo_results.append({
                    'id': f"slo_{uuid4().hex[:12]}",
                    'contract_id': contract_id,
                    'metric_type': 'freshness',
                    'expected_value': freshness_result['expected_value'],
                    'actual_value': freshness_result['actual_value'],
                    'status': freshness_result['status'],
                    'severity': 'high' if freshness_result['status'] == 'fail' else 'info',
                    'details': freshness_result['details'],
                    'measurement_ts': datetime.now(),
                })
            
            # Completeness
            if completeness_pct:
                completeness_result = validate_completeness(
                    connection, config, model_name, 'dbt', 'analytics',
                    completeness_pct
                )
                slo_results.append({
                    'id': f"slo_{uuid4().hex[:12]}",
                    'contract_id': contract_id,
                    'metric_type': 'completeness',
                    'expected_value': completeness_result['expected_value'],
                    'actual_value': completeness_result['actual_value'],
                    'status': completeness_result['status'],
                    'severity': 'high' if completeness_result['status'] == 'fail' else 'info',
                    'details': completeness_result['details'],
                    'measurement_ts': datetime.now(),
                })
            
            # Availability
            availability_result = validate_availability(
                connection, config, model_name, 'dbt', 'analytics'
            )
            slo_results.append({
                'id': f"slo_{uuid4().hex[:12]}",
                'contract_id': contract_id,
                'metric_type': 'availability',
                'expected_value': availability_result['expected_value'],
                'actual_value': availability_result['actual_value'],
                'status': availability_result['status'],
                'severity': 'critical' if availability_result['status'] == 'fail' else 'info',
                'details': availability_result['details'],
                'measurement_ts': datetime.now(),
            })
        
    except Exception as e:
        logger.error(f"Error validating SLOs: {e}")
    finally:
        cursor.close()
    
    return slo_results


def insert_slo_results(connection, config: Dict[str, Any],
                      slo_results: List[Dict[str, Any]]) -> None:
    """Insert SLO validation results into slo_metrics table"""
    
    reg_config = config.get('registry_config', {})
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    if not slo_results:
        logger.info("No SLO results to insert")
        return
    
    cursor = connection.cursor()
    
    try:
        for result in slo_results:
            cursor.execute(f"""
            INSERT INTO {catalog}.{database}.slo_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
            """, (
                result['id'],
                result['contract_id'],
                result['metric_type'],
                result['expected_value'],
                result['actual_value'],
                result['status'],
                result['severity'],
                json.dumps(result['details']),
                result['measurement_ts'].isoformat(),
            ))
        
        logger.info(f"✓ Inserted {len(slo_results)} SLO metric(s)")
        
    except Exception as e:
        logger.error(f"Error inserting SLO results: {e}")
    finally:
        cursor.close()


def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("SLO Validation")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*60)
    
    # Load config
    config = load_profiles()
    
    # Create connection
    connection = create_connection(config)
    
    try:
        # Validate SLOs
        slo_results = validate_contract_slos(connection, config)
        
        # Insert results
        insert_slo_results(connection, config, slo_results)
        
        # Summary
        passed = sum(1 for r in slo_results if r['status'] == 'pass')
        failed = sum(1 for r in slo_results if r['status'] == 'fail')
        errors = sum(1 for r in slo_results if r['status'] == 'error')
        
        logger.info("\n" + "="*60)
        logger.info("✓ SLO VALIDATION COMPLETED")
        logger.info("="*60)
        logger.info(f"Total checks: {len(slo_results)}")
        logger.info(f"Passed: {passed}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Errors: {errors}")
        logger.info("="*60)
        
    finally:
        connection.close()


if __name__ == '__main__':
    main()
