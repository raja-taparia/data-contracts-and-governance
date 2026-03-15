#!/usr/bin/env python3
"""
Daily Validation Orchestrator

Runs the complete validation pipeline in sequence:
1. extract_contracts.py — Refresh contract definitions from YAML
2. extract_models.py — Refresh model definitions from dbt YAML
3. dbt test — Execute all dbt tests
4. log_test_results.py — Denormalize and log test results
5. validate_slos.py — Check SLO metrics
6. validate_data_quality.py — Check data quality
7. update_subscriber_status.py — Aggregate status and set ready_for_consumption flags

Can be run manually or scheduled as a daily cron job.
All configuration comes from profiles.yml (validation_config section).

Exit codes:
- 0: All steps successful
- 1: One or more steps failed (non-blocking)
- 2: Critical step failed (blocking)
"""

import sys
import os
import subprocess
import logging
from datetime import datetime
from typing import Dict, Any
import yaml

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
    
    return config


def run_command(cmd: str, description: str, critical: bool = False, cwd: str = None) -> bool:
    """Run shell command and return success status"""
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Step: {description}")
    logger.info(f"{'='*60}")
    logger.info(f"Command: {cmd}")
    
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd or os.path.dirname(__file__),
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            logger.info(f"✓ {description} completed successfully")
            return True
        else:
            status = "CRITICAL" if critical else "WARNING"
            logger.error(f"✗ {description} failed with exit code {result.returncode} [{status}]")
            return False
            
    except Exception as e:
        status = "CRITICAL" if critical else "WARNING"
        logger.error(f"✗ Error running {description}: {e} [{status}]")
        return False


def main():
    """Main orchestration"""
    
    logger.info("="*60)
    logger.info("Daily Validation Orchestration")
    logger.info(f"Started: {datetime.now().isoformat()}")
    logger.info("="*60)
    
    # Load config
    config = load_profiles()
    val_config = config.get('validation_config', {})
    dbt_config = config.get('dbt_config', {})
    
    # Determine project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Track results
    results = {}
    critical_failed = False
    
    # Step 1: Extract Contracts
    if val_config.get('enable_contract_validation', True):
        results['extract_contracts'] = run_command(
            f"python {project_root}/scripts/extract_contracts.py",
            "Extract Contract Definitions",
            critical=False,
            cwd=project_root
        )
    else:
        logger.info("✓ Skipped: Extract Contracts (disabled)")
        results['extract_contracts'] = None
    
    # Step 2: Extract Models
    if val_config.get('enable_contract_validation', True):
        results['extract_models'] = run_command(
            f"python {project_root}/scripts/extract_models.py",
            "Extract Model Definitions",
            critical=False,
            cwd=project_root
        )
    else:
        logger.info("✓ Skipped: Extract Models (disabled)")
        results['extract_models'] = None
    
    # Step 3: Run dbt tests
    results['dbt_test'] = run_command(
        f"dbt test",
        "Run dbt Tests",
        critical=True,
        cwd=f"{project_root}/dbt"
    )
    if not results['dbt_test']:
        critical_failed = True
    
    # Step 4: Log Test Results
    if results['dbt_test'] or results['dbt_test'] is None:
        results['log_test_results'] = run_command(
            f"python {project_root}/scripts/log_test_results.py",
            "Log Test Results to Databricks",
            critical=False,
            cwd=project_root
        )
    else:
        logger.info("✗ Skipped: Log Test Results (dbt test failed)")
        results['log_test_results'] = False
    
    # Step 5: Validate SLOs
    if val_config.get('enable_slo_validation', True):
        results['validate_slos'] = run_command(
            f"python {project_root}/scripts/validate_slos.py",
            "Validate SLO Metrics",
            critical=False,
            cwd=project_root
        )
    else:
        logger.info("✓ Skipped: Validate SLOs (disabled)")
        results['validate_slos'] = None
    
    # Step 6: Validate Data Quality
    if val_config.get('enable_data_quality_validation', True):
        results['validate_data_quality'] = run_command(
            f"python {project_root}/scripts/validate_data_quality.py",
            "Validate Data Quality",
            critical=False,
            cwd=project_root
        )
    else:
        logger.info("✓ Skipped: Validate Data Quality (disabled)")
        results['validate_data_quality'] = None
    
    # Step 7: Update Subscriber Status
    results['update_subscriber_status'] = run_command(
        f"python {project_root}/scripts/update_subscriber_status.py",
        "Update Subscriber Status",
        critical=False,
        cwd=project_root
    )
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("ORCHESTRATION SUMMARY")
    logger.info("="*60)
    
    for step, success in results.items():
        if success is None:
            status_str = "SKIPPED"
        elif success:
            status_str = "✓ PASS"
        else:
            status_str = "✗ FAIL"
        logger.info(f"{step:.<40} {status_str}")
    
    logger.info("="*60)
    
    # Determine exit code
    passed = sum(1 for s in results.values() if s is True)
    failed = sum(1 for s in results.values() if s is False)
    skipped = sum(1 for s in results.values() if s is None)
    
    logger.info(f"\nResults: {passed} passed, {failed} failed, {skipped} skipped")
    logger.info(f"Completed: {datetime.now().isoformat()}")
    
    if critical_failed:
        logger.error("\n✗ Critical step failed - validation pipeline halted")
        return 2
    elif failed > 0:
        logger.warning(f"\n⚠ {failed} non-critical step(s) failed")
        return 1
    else:
        logger.info("\n✓ Validation pipeline completed successfully")
        return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
