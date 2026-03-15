#!/usr/bin/env python3
"""
Subscriber Status Aggregation and Ready-for-Consumption Flag

For each subscriber:
1. Get watched contract IDs
2. Aggregate test results (today's runs)
3. Aggregate SLO metrics (today's checks)
4. Aggregate data quality issues (today's detected)
5. Calculate: ready_for_consumption = all_tests_pass && all_slos_pass && no_critical_issues
6. Update subscribers table with flag
7. If webhook configured: POST notification to webhook

Subscribers watch contracts and receive ready-for-consumption updates.
Can subscribe to multiple contracts (comma-separated IDs).
"""

import sys
import os
import json
import logging
from datetime import datetime, date
from typing import Dict, Any, List
import yaml
from uuid import uuid4
from databricks import sql
import requests

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


def get_contract_test_status(connection, config: Dict[str, Any],
                            contract_id: str, check_date: date) -> Dict[str, Any]:
    """Get test status for contract (today's results)"""
    
    reg_config = config.get('registry_config', {})
    reg_catalog = reg_config.get('catalog', 'dbt')
    reg_database = reg_config.get('database', 'governance')
    
    cursor = connection.cursor()
    
    try:
        cursor.execute(f"""
        SELECT 
            COUNT(*) as total_tests,
            SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passing,
            SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failing
        FROM {reg_catalog}.{reg_database}.test_results
        WHERE contract_id = '{contract_id}'
            AND DATE(run_timestamp) = '{check_date}'
        """)
        
        result = cursor.fetchone()
        if result:
            total, passing, failing = result
            return {
                'total_tests': total or 0,
                'passing_tests': passing or 0,
                'failing_tests': failing or 0,
                'status': 'pass' if (failing or 0) == 0 else 'fail',
            }
        
        return {'total_tests': 0, 'passing_tests': 0, 'failing_tests': 0, 'status': 'unknown'}
        
    except Exception as e:
        logger.warning(f"Could not get test status: {e}")
        return {'status': 'error', 'error': str(e)}
    finally:
        cursor.close()


def get_contract_slo_status(connection, config: Dict[str, Any],
                           contract_id: str, check_date: date) -> Dict[str, Any]:
    """Get SLO status for contract (today's results)"""
    
    reg_config = config.get('registry_config', {})
    reg_catalog = reg_config.get('catalog', 'dbt')
    reg_database = reg_config.get('database', 'governance')
    
    cursor = connection.cursor()
    
    try:
        cursor.execute(f"""
        SELECT 
            COUNT(*) as total_slos,
            SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) as passing,
            SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END) as failing
        FROM {reg_catalog}.{reg_database}.slo_metrics
        WHERE contract_id = '{contract_id}'
            AND DATE(measurement_ts) = '{check_date}'
        """)
        
        result = cursor.fetchone()
        if result:
            total, passing, failing = result
            return {
                'total_slos': total or 0,
                'passing_slos': passing or 0,
                'failing_slos': failing or 0,
                'status': 'pass' if (failing or 0) == 0 else 'fail',
            }
        
        return {'total_slos': 0, 'passing_slos': 0, 'failing_slos': 0, 'status': 'unknown'}
        
    except Exception as e:
        logger.warning(f"Could not get SLO status: {e}")
        return {'status': 'error', 'error': str(e)}
    finally:
        cursor.close()


def get_contract_dq_status(connection, config: Dict[str, Any],
                          contract_id: str, check_date: date) -> Dict[str, Any]:
    """Get data quality status for contract models (today's issues)"""
    
    reg_config = config.get('registry_config', {})
    reg_catalog = reg_config.get('catalog', 'dbt')
    reg_database = reg_config.get('database', 'governance')
    
    cursor = connection.cursor()
    
    try:
        cursor.execute(f"""
        SELECT 
            COUNT(*) as total_issues,
            SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_issues,
            SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as high_issues
        FROM {reg_catalog}.{reg_database}.data_quality_log dql
        JOIN {reg_catalog}.{reg_database}.models m ON dql.model_id = m.id
        WHERE m.contract_id = '{contract_id}'
            AND DATE(dql.detected_at) = '{check_date}'
            AND dql.reviewed_flag = FALSE
        """)
        
        result = cursor.fetchone()
        if result:
            total, critical, high = result
            return {
                'total_issues': total or 0,
                'critical_issues': critical or 0,
                'high_issues': high or 0,
                'status': 'fail' if (critical or 0) > 0 else ('warning' if (high or 0) > 0 else 'pass'),
            }
        
        return {'total_issues': 0, 'critical_issues': 0, 'high_issues': 0, 'status': 'pass'}
        
    except Exception as e:
        logger.warning(f"Could not get DQ status: {e}")
        return {'status': 'error', 'error': str(e)}
    finally:
        cursor.close()


def calculate_ready_for_consumption(test_status: Dict, slo_status: Dict, 
                                   dq_status: Dict) -> tuple:
    """Calculate if contract is ready for consumption"""
    
    # Ready only if all conditions pass
    tests_ok = test_status.get('status') != 'fail'
    slos_ok = slo_status.get('status') != 'fail'
    dq_ok = dq_status.get('status') != 'fail'
    
    ready = tests_ok and slos_ok and dq_ok
    
    reason = []
    if not tests_ok:
        reason.append(f"Tests failing: {test_status.get('failing_tests', 0)} failures")
    if not slos_ok:
        reason.append(f"SLOs failing: {slo_status.get('failing_slos', 0)} failures")
    if not dq_ok:
        reason.append(f"Data quality issues: {dq_status.get('critical_issues', 0)} critical")
    
    reason_text = "; ".join(reason) if reason else "All checks passed"
    
    return ready, reason_text


def send_webhook_notification(webhook_url: str, subscriber: Dict, 
                            ready_flag: bool, reason: str, config: Dict) -> bool:
    """Send webhook notification if enabled"""
    
    notif_config = config.get('notification_config', {})
    if not notif_config.get('enable_webhooks') or not webhook_url:
        return True
    
    try:
        payload = {
            'subscriber_id': subscriber.get('id'),
            'subscriber_name': subscriber.get('subscriber_name'),
            'ready_for_consumption': ready_flag,
            'reason': reason,
            'timestamp': datetime.now().isoformat(),
            'watched_contracts': subscriber.get('watching_contract_ids_json'),
        }
        
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=notif_config.get('webhook_timeout_seconds', 10)
        )
        
        if response.status_code == 200:
            logger.info(f"✓ Webhook notification sent to {webhook_url}")
            return True
        else:
            logger.warning(f"Webhook failed: {response.status_code}")
            return False
        
    except Exception as e:
        logger.warning(f"Could not send webhook: {e}")
        return False


def update_subscriber_status(connection, config: Dict[str, Any]) -> None:
    """Update ready_for_consumption flag for all subscribers"""
    
    reg_config = config.get('registry_config', {})
    reg_catalog = reg_config.get('catalog', 'dbt')
    reg_database = reg_config.get('database', 'governance')
    today = date.today()
    
    cursor = connection.cursor()
    
    try:
        # Get all subscribers
        cursor.execute(f"""
        SELECT id, subscriber_name, watching_contract_ids_json, webhook_url, email, slack_channel
        FROM {reg_catalog}.{reg_database}.subscribers
        """)
        
        subscribers = cursor.fetchall()
        
        for sub_id, sub_name, watching_ids_json, webhook_url, email, slack_ch in subscribers:
            logger.info(f"Updating status for subscriber: {sub_name}")
            
            # Parse watched contract IDs
            try:
                contract_ids = json.loads(watching_ids_json) if watching_ids_json else []
            except:
                contract_ids = []
            
            if not contract_ids:
                logger.info(f"  Subscriber {sub_name} is not watching any contracts")
                continue
            
            # Aggregate status for all watched contracts
            all_ready = True
            all_reasons = []
            
            for contract_id in contract_ids:
                test_status = get_contract_test_status(connection, config, contract_id, today)
                slo_status = get_contract_slo_status(connection, config, contract_id, today)
                dq_status = get_contract_dq_status(connection, config, contract_id, today)
                
                ready, reason = calculate_ready_for_consumption(test_status, slo_status, dq_status)
                
                if not ready:
                    all_ready = False
                    all_reasons.append(f"{contract_id}: {reason}")
                else:
                    all_reasons.append(f"{contract_id}: Ready")
            
            reason_text = "; ".join(all_reasons) if all_reasons else "All contracts ready"
            
            # Update subscriber
            cursor.execute(f"""
            UPDATE {reg_catalog}.{reg_database}.subscribers
            SET 
                ready_for_consumption = {all_ready},
                ready_for_consumption_reason = '{reason_text}',
                daily_slo_check_ts = CURRENT_TIMESTAMP(),
                _updated_ts = CURRENT_TIMESTAMP()
            WHERE id = '{sub_id}'
            """)
            
            logger.info(f"  ✓ {sub_name}: ready={all_ready}")
            
            # Send webhook if enabled
            if config.get('notification_config', {}).get('enable_webhooks') and webhook_url:
                subscriber = {
                    'id': sub_id,
                    'subscriber_name': sub_name,
                    'watching_contract_ids_json': watching_ids_json,
                }
                send_webhook_notification(webhook_url, subscriber, all_ready, reason_text, config)
        
    except Exception as e:
        logger.error(f"Error updating subscriber status: {e}")
    finally:
        cursor.close()


def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("Subscriber Status Aggregation")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("="*60)
    
    # Load config
    config = load_profiles()
    
    # Create connection
    connection = create_connection(config)
    
    try:
        # Update subscriber status
        update_subscriber_status(connection, config)
        
        logger.info("\n" + "="*60)
        logger.info("✓ SUBSCRIBER STATUS UPDATED")
        logger.info("="*60)
        
    finally:
        connection.close()


if __name__ == '__main__':
    main()
