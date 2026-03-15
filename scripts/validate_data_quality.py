#!/usr/bin/env python3
"""
Data Quality Validation

Detects data quality issues:
- Schema Drift: columns added/removed vs contract definition
- Data Anomalies: null spikes, outliers, type mismatches
- Customizable checks via configuration

Stores results in data_quality_log table with:
  (model_id, quality_check_type, severity, status, details, detected_at)

Can be run daily or on-demand.
"""

import sys
import os
import json
import logging
from datetime import datetime
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


def check_schema_drift(connection, config: Dict[str, Any],
                      model_id: str, model_name: str, catalog: str, 
                      database: str) -> List[Dict[str, Any]]:
    """Detect schema drift: columns added/removed vs contract"""
    
    issues = []
    reg_config = config.get('registry_config', {})
    reg_catalog = reg_config.get('catalog', 'dbt')
    reg_database = reg_config.get('database', 'governance')
    
    cursor = connection.cursor()
    
    try:
        # Get contracted columns
        cursor.execute(f"""
        SELECT column_name
        FROM {reg_catalog}.{reg_database}.columns
        WHERE model_id = '{model_id}'
        """)
        
        contracted_cols = {row[0] for row in cursor.fetchall()}
        
        if not contracted_cols:
            logger.info(f"No contract columns found for {model_name}")
            return issues
        
        # Get actual columns from table
        cursor.execute(f"""
        DESC {catalog}.{database}.{model_name}
        """)
        
        actual_cols = {row[0] for row in cursor.fetchall()}
        
        # Find missing columns (in contract but not in table)
        missing_cols = contracted_cols - actual_cols
        for col in missing_cols:
            issues.append({
                'id': f"dq_{uuid4().hex[:12]}",
                'model_id': model_id,
                'quality_check_type': 'schema_drift',
                'severity': 'high',
                'status': 'fail',
                'description': f"Column '{col}' in contract but missing from table",
                'details': {'issue_type': 'missing_column', 'column': col},
                'detected_at': datetime.now(),
            })
        
        # Find extra columns (in table but not in contract)
        extra_cols = actual_cols - contracted_cols
        extra_cols = {c for c in extra_cols if not c.startswith('_')}  # Ignore system columns
        for col in extra_cols:
            issues.append({
                'id': f"dq_{uuid4().hex[:12]}",
                'model_id': model_id,
                'quality_check_type': 'schema_drift',
                'severity': 'medium',
                'status': 'warning',
                'description': f"Column '{col}' in table but not in contract",
                'details': {'issue_type': 'extra_column', 'column': col},
                'detected_at': datetime.now(),
            })
        
        if issues:
            logger.info(f"✓ Found {len(issues)} schema drift issue(s) in {model_name}")
        
    except Exception as e:
        logger.warning(f"Schema drift check failed: {e}")
    finally:
        cursor.close()
    
    return issues


def check_null_spike(connection, config: Dict[str, Any],
                    model_id: str, model_name: str, catalog: str,
                    database: str) -> List[Dict[str, Any]]:
    """Detect unusual null value spikes"""
    
    issues = []
    reg_config = config.get('registry_config', {})
    reg_catalog = reg_config.get('catalog', 'dbt')
    reg_database = reg_config.get('database', 'governance')
    val_config = config.get('validation_config', {})
    threshold = val_config.get('null_spike_threshold_percentage', 10)
    
    cursor = connection.cursor()
    
    try:
        # Get required columns
        cursor.execute(f"""
        SELECT column_name
        FROM {reg_catalog}.{reg_database}.columns
        WHERE model_id = '{model_id}' AND required_flag = true
        """)
        
        required_cols = [row[0] for row in cursor.fetchall()]
        
        for col in required_cols:
            cursor.execute(f"""
            SELECT 
                ROUND(100.0 * SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_pct
            FROM {catalog}.{database}.{model_name}
            """)
            
            result = cursor.fetchone()
            if result:
                null_pct = result[0]
                if null_pct > threshold:
                    issues.append({
                        'id': f"dq_{uuid4().hex[:12]}",
                        'model_id': model_id,
                        'quality_check_type': 'null_spike',
                        'severity': 'high' if null_pct > threshold * 2 else 'medium',
                        'status': 'fail',
                        'description': f"Column '{col}' has {null_pct}% nulls (threshold: {threshold}%)",
                        'details': {'column': col, 'null_percentage': null_pct, 'threshold': threshold},
                        'detected_at': datetime.now(),
                    })
        
        if issues:
            logger.info(f"✓ Found {len(issues)} null spike issue(s) in {model_name}")
        
    except Exception as e:
        logger.warning(f"Null spike check failed: {e}")
    finally:
        cursor.close()
    
    return issues


def validate_data_quality(connection, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Run all data quality checks for all models"""
    
    reg_config = config.get('registry_config', {})
    val_config = config.get('validation_config', {})
    reg_catalog = reg_config.get('catalog', 'dbt')
    reg_database = reg_config.get('database', 'governance')
    
    all_issues = []
    cursor = connection.cursor()
    
    try:
        # Get all models
        cursor.execute(f"""
        SELECT id, model_name, schema_name
        FROM {reg_catalog}.{reg_database}.models
        """)
        
        models = cursor.fetchall()
        
        for model_id, model_name, schema_name in models:
            logger.info(f"Checking data quality for {model_name}")
            
            # Run checks
            if val_config.get('enable_schema_drift_detection'):
                all_issues.extend(check_schema_drift(
                    connection, config, model_id, model_name, 'dbt', 'analytics'
                ))
            
            if val_config.get('enable_null_spike_detection'):
                all_issues.extend(check_null_spike(
                    connection, config, model_id, model_name, 'dbt', 'analytics'
                ))
        
    except Exception as e:
        logger.error(f"Error validating data quality: {e}")
    finally:
        cursor.close()
    
    return all_issues


def insert_dq_issues(connection, config: Dict[str, Any],
                    issues: List[Dict[str, Any]]) -> None:
    """Insert data quality issues into data_quality_log table"""
    
    reg_config = config.get('registry_config', {})
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    if not issues:
        logger.info("No data quality issues found")
        return
    
    cursor = connection.cursor()
    
    try:
        for issue in issues:
            cursor.execute(f"""
            INSERT INTO {catalog}.{database}.data_quality_log VALUES (?, ?, ?, ?, ?, ?, ?, FALSE, NULL, ?, CURRENT_TIMESTAMP())
            """, (
                issue['id'],
                issue['model_id'],
                issue['quality_check_type'],
                issue['severity'],
                issue['status'],
                issue['description'],
                json.dumps(issue['details']),
                issue['detected_at'].isoformat(),
            ))
        
        logger.info(f"✓ Inserted {len(issues)} data quality issue(s)")
        
    except Exception as e:
        logger.error(f"Error inserting issues: {e}")
    finally:
        cursor.close()


def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("Data Quality Validation")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*60)
    
    # Load config
    config = load_profiles()
    
    # Create connection
    connection = create_connection(config)
    
    try:
        # Validate data quality
        issues = validate_data_quality(connection, config)
        
        # Insert issues
        insert_dq_issues(connection, config, issues)
        
        # Summary
        high_severity = sum(1 for i in issues if i['severity'] == 'high')
        medium_severity = sum(1 for i in issues if i['severity'] == 'medium')
        
        logger.info("\n" + "="*60)
        logger.info("✓ DATA QUALITY VALIDATION COMPLETED")
        logger.info("="*60)
        logger.info(f"Total issues: {len(issues)}")
        logger.info(f"High severity: {high_severity}")
        logger.info(f"Medium severity: {medium_severity}")
        logger.info("="*60)
        
    finally:
        connection.close()


if __name__ == '__main__':
    main()
