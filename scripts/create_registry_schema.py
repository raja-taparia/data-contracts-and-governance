#!/usr/bin/env python3
"""
Create Databricks Registry Schema for Contract Governance System

This script initializes all required tables in Databricks for storing:
- Contract definitions and metadata
- Model and column registry
- Test definitions and results
- Publisher and subscriber information
- SLO metrics and data quality logs

Tables created:
1. contracts - Contract definitions with SLOs
2. models - Model metadata linked to contracts
3. columns - Column definitions with data types
4. tests - Test definitions (not_null, unique, slo, etc.)
5. test_results - Denormalized test execution results (partitioned by timestamp)
6. publishers - Data publishers
7. subscribers - Data subscribers watching contracts
8. slo_metrics - SLO performance measurements (partitioned by timestamp)
9. data_quality_log - Data quality issues detected (partitioned by timestamp)

Configuration is read from dbt/profiles.yml via yaml. All Databricks connection
details come from the secrets section.
"""

import sys
import os
import logging
from datetime import datetime
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
    
    # Resolve environment variables in config
    def resolve_env_vars(value):
        if isinstance(value, str) and value.startswith("{{ env_var("):
            # Parse {{ env_var('VAR_NAME', 'default_value') }}
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
    """Create Databricks SQL connection from config"""
    secrets = config.get('secrets', {})
    
    host = secrets.get('databricks_host')
    http_path = secrets.get('databricks_http_path')
    token = secrets.get('databricks_token')
    
    if not all([host, http_path, token]):
        logger.error("Missing Databricks credentials in secrets section of profiles.yml")
        sys.exit(1)
    
    logger.info(f"Connecting to Databricks at {host}")
    
    try:
        connection = sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
            catalog='hive_metastore',  # Will switch to UC catalog in statements
        )
        logger.info("✓ Connected to Databricks")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to Databricks: {e}")
        sys.exit(1)


def get_registry_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract registry configuration"""
    return config.get('registry_config', {})


def create_tables(connection, config: Dict[str, Any]) -> None:
    """Create all registry schema tables"""
    reg_config = get_registry_config(config)
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    logger.info(f"Creating schema in catalog '{catalog}', database '{database}'")
    
    cursor = connection.cursor()
    
    try:
        # Create database if not exists
        create_db_sql = f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}"
        cursor.execute(create_db_sql)
        logger.info(f"✓ Database {catalog}.{database} ready")
        
        # 1. CONTRACTS TABLE
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.contracts (
            id STRING NOT NULL,
            name STRING NOT NULL,
            version STRING NOT NULL,
            domain STRING NOT NULL,
            owner_name STRING,
            owner_email STRING,
            owner_slack STRING,
            source_path STRING,
            description STRING,
            slo_freshness_hours INT,
            slo_completeness DECIMAL(5, 2),
            slo_accuracy DECIMAL(5, 2),
            slo_availability DECIMAL(5, 2),
            published_at TIMESTAMP,
            published_by STRING,
            contract_json STRING,
            _created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            _updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (id, version)
        )
        USING DELTA
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: contracts")
        
        # 2. MODELS TABLE
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.models (
            id STRING NOT NULL,
            model_name STRING NOT NULL,
            schema_name STRING NOT NULL,
            contract_id STRING,
            materialized_as STRING,
            freshness_slo_hours INT,
            completeness_slo DECIMAL(5, 2),
            critical BOOLEAN DEFAULT false,
            pii_flag BOOLEAN DEFAULT false,
            owner_name STRING,
            meta_json STRING,
            _created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            _updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (id)
        )
        USING DELTA
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: models")
        
        # 3. COLUMNS TABLE
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.columns (
            id STRING NOT NULL,
            model_id STRING NOT NULL,
            column_name STRING NOT NULL,
            data_type STRING,
            required_flag BOOLEAN,
            description STRING,
            enum_values_json STRING,
            _created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (id)
        )
        USING DELTA
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: columns")
        
        # 4. TESTS TABLE
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.tests (
            id STRING NOT NULL,
            test_name STRING NOT NULL,
            test_type STRING NOT NULL,
            column_id STRING,
            model_id STRING NOT NULL,
            severity STRING DEFAULT 'error',
            test_params_json STRING,
            description STRING,
            _created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (id)
        )
        USING DELTA
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: tests")
        
        # 5. TEST_RESULTS TABLE (denormalized for easy querying)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.test_results (
            id STRING NOT NULL,
            run_id STRING NOT NULL,
            test_id STRING NOT NULL,
            contract_id STRING,
            model_name STRING NOT NULL,
            column_name STRING,
            test_name STRING NOT NULL,
            test_type STRING NOT NULL,
            status STRING NOT NULL,
            failure_message STRING,
            rows_affected INT,
            execution_time_ms INT,
            compiled_sql STRING,
            run_timestamp TIMESTAMP NOT NULL,
            configured_by STRING,
            _inserted_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        USING DELTA
        CLUSTER BY (run_timestamp)
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: test_results (clustered)")
        
        # 6. PUBLISHERS TABLE
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.publishers (
            id STRING NOT NULL,
            publisher_type STRING NOT NULL,
            name STRING NOT NULL,
            description STRING,
            config_json STRING,
            last_publish_at TIMESTAMP,
            last_status STRING,
            _created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            _updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (id)
        )
        USING DELTA
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: publishers")
        
        # 7. SUBSCRIBERS TABLE
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.subscribers (
            id STRING NOT NULL,
            subscriber_name STRING NOT NULL,
            email STRING,
            slack_channel STRING,
            webhook_url STRING,
            watching_contract_ids_json STRING,
            ready_for_consumption BOOLEAN DEFAULT false,
            ready_for_consumption_reason STRING,
            daily_slo_check_ts TIMESTAMP,
            _created_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            _updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (id)
        )
        USING DELTA
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: subscribers")
        
        # 8. SLO_METRICS TABLE (denormalized for easy querying)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.slo_metrics (
            id STRING NOT NULL,
            contract_id STRING NOT NULL,
            metric_type STRING NOT NULL,
            expected_value DECIMAL(10, 4),
            actual_value DECIMAL(10, 4),
            status STRING NOT NULL,
            severity STRING,
            measurement_details_json STRING,
            measurement_ts TIMESTAMP NOT NULL,
            _inserted_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        USING DELTA
        CLUSTER BY (measurement_ts)
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: slo_metrics (clustered)")
        
        # 9. DATA_QUALITY_LOG TABLE
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{database}.data_quality_log (
            id STRING NOT NULL,
            model_id STRING NOT NULL,
            quality_check_type STRING NOT NULL,
            severity STRING NOT NULL,
            status STRING NOT NULL,
            description STRING,
            details_json STRING,
            reviewed_flag BOOLEAN DEFAULT false,
            reviewer_name STRING,
            detected_at TIMESTAMP NOT NULL,
            _inserted_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
        USING DELTA
        CLUSTER BY (detected_at)
        TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
        """)
        logger.info("✓ Table: data_quality_log (clustered)")
        
        logger.info("\n" + "="*60)
        logger.info("✓ ALL TABLES CREATED SUCCESSFULLY")
        logger.info("="*60)
        logger.info(f"Location: {catalog}.{database}")
        logger.info(f"Total tables: 9")
        logger.info(f"  - contracts (contracts, models, columns, tests metadata)")
        logger.info(f"  - test_results (denormalized test execution results)")
        logger.info(f"  - publishers, subscribers (lineage)")
        logger.info(f"  - slo_metrics, data_quality_log (validation results)")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        cursor.close()


def create_indexes_and_views(connection, config: Dict[str, Any]) -> None:
    """Create useful indexes and views for querying"""
    reg_config = get_registry_config(config)
    catalog = reg_config.get('catalog', 'dbt')
    database = reg_config.get('database', 'governance')
    
    cursor = connection.cursor()
    
    try:
        # View: Latest test results for each test
        cursor.execute(f"""
        CREATE OR REPLACE VIEW {catalog}.{database}.v_latest_test_results AS
        SELECT 
            test_name,
            model_name,
            column_name,
            status,
            failure_message,
            execution_time_ms,
            run_timestamp
        FROM {catalog}.{database}.test_results
        WHERE QUALIFY ROW_NUMBER() OVER (PARTITION BY test_id ORDER BY run_timestamp DESC) = 1
        """)
        logger.info("✓ View: v_latest_test_results")
        
        # View: Contract health dashboard
        cursor.execute(f"""
        CREATE OR REPLACE VIEW {catalog}.{database}.v_contract_health AS
        SELECT 
            c.name,
            c.domain,
            COUNT(DISTINCT t.id) as total_tests,
            SUM(CASE WHEN tr.status = 'pass' THEN 1 ELSE 0 END) as passing_tests,
            SUM(CASE WHEN tr.status = 'fail' THEN 1 ELSE 0 END) as failing_tests,
            SUM(CASE WHEN tr.status = 'skipped' THEN 1 ELSE 0 END) as skipped_tests,
            MAX(tr.run_timestamp) as last_test_run,
            ROUND(100.0 * SUM(CASE WHEN tr.status = 'pass' THEN 1 ELSE 0 END) / COUNT(DISTINCT t.id), 2) as pass_rate
        FROM {catalog}.{database}.contracts c
        LEFT JOIN {catalog}.{database}.models m ON c.id = m.contract_id
        LEFT JOIN {catalog}.{database}.tests t ON m.id = t.model_id
        LEFT JOIN {catalog}.{database}.test_results tr ON t.id = tr.test_id
        GROUP BY c.id, c.name, c.domain
        """)
        logger.info("✓ View: v_contract_health")
        
        # View: Subscriber readiness
        cursor.execute(f"""
        CREATE OR REPLACE VIEW {catalog}.{database}.v_subscriber_readiness AS
        SELECT 
            subscriber_name,
            ready_for_consumption,
            ready_for_consumption_reason,
            daily_slo_check_ts,
            _updated_ts
        FROM {catalog}.{database}.subscribers
        ORDER BY _updated_ts DESC
        """)
        logger.info("✓ View: v_subscriber_readiness")
        
    except Exception as e:
        logger.warning(f"Could not create views (may already exist): {e}")
    finally:
        cursor.close()


def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("Databricks Registry Schema Setup")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*60)
    
    # Load configuration
    config = load_profiles()
    
    # Create connection
    connection = create_connection(config)
    
    try:
        # Create tables
        create_tables(connection, config)
        
        # Create views
        create_indexes_and_views(connection, config)
        
        logger.info("\n✓ Registry schema setup completed successfully!")
        logger.info("\nNext steps:")
        logger.info("1. Run: python scripts/extract_contracts.py")
        logger.info("2. Run: python scripts/extract_models.py")
        logger.info("3. Run: dbt test (this will populate test_results)")
        logger.info("4. Run: python scripts/log_test_results.py")
        
    finally:
        connection.close()


if __name__ == '__main__':
    main()
