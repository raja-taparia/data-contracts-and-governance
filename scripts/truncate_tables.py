#!/usr/bin/env python3
"""
Truncate All Governance Tables

This script truncates all tables in the dbt.governance database.
This is useful for cleaning up the environment and starting from a fresh state.
"""

import sys
import os
import logging
from typing import Dict, Any
import yaml
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


def truncate_tables(connection, config: Dict[str, Any]):
    """Truncate all governance tables"""
    
    reg_config = config.get('registry_config', {})
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    tables_to_truncate = [
        "contracts",
        "models",
        "columns",
        "tests",
        "data_quality_log",
        "publishers",
        "slo_metrics",
        "subscribers",
        "test_results"
    ]
    
    cursor = connection.cursor()
    
    try:
        for table in tables_to_truncate:
            logger.info(f"Truncating table: {catalog}.{database}.{table}")
            cursor.execute(f"TRUNCATE TABLE {catalog}.{database}.{table}")
        
        logger.info("✓ All governance tables truncated successfully")
        
    except Exception as e:
        logger.error(f"Error truncating tables: {e}")
    finally:
        cursor.close()


def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("Truncating All Governance Tables")
    logger.info("="*60)
    
    # Load config
    config = load_profiles()
    
    # Create connection
    connection = create_connection(config)
    
    try:
        truncate_tables(connection, config)
    finally:
        connection.close()


if __name__ == '__main__':
    main()
