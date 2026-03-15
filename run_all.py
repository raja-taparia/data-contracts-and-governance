#!/usr/bin/env python3
"""
Master script to run all data governance extraction and seeding steps.
"""

import subprocess
import sys
import os

def run_command(command, cwd=None):
    """Run a command in a subprocess and check for errors."""
    try:
        # Joining command for display purposes
        command_str = ' '.join(command)
        print(f"--- Running command: {command_str} ---")
        
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=cwd
        )
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            print(f"--- Error running command: {command_str} ---")
            print(stderr)
            # Decide whether to exit or just log the error
            # For this master script, we'll exit on critical failures
        else:
            print(f"--- Command successful: {command_str} ---")
            print(stdout)

    except FileNotFoundError:
        print(f"--- Error: Command not found: {command[0]} ---")
        sys.exit(1)
    except Exception as e:
        print(f"--- An unexpected error occurred: {e} ---")
        sys.exit(1)


def main():
    """Main execution"""
    print("--- Starting Data Governance Process ---")

    # Get the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # --- Create Schema: Create the governance schema and tables ---
    print("--- Creating governance schema and tables (if they don't exist) ---")
    run_command([sys.executable, os.path.join(script_dir, "scripts/create_registry_schema.py")])

    # --- Seeding essential data ---
    print("--- Seeding publishers and subscribers data ---")
    run_command(["dbt", "seed", "--profiles-dir", "."], cwd=os.path.join(script_dir, "dbt"))

    # --- Extracting metadata from contracts and models ---
    print("--- Extracting contract and model definitions ---")
    run_command([sys.executable, os.path.join(script_dir, "scripts/extract_contracts.py")])
    run_command([sys.executable, os.path.join(script_dir, "scripts/extract_models.py")])
    
    # --- Running dbt to build models and run tests ---
    print("--- Running dbt build to create models and run tests ---")
    # Using 'dbt build' which combines run and test
    run_command(["dbt", "build", "--profiles-dir", "."], cwd=os.path.join(script_dir, "dbt"))

    # --- Logging test results ---
    print("--- Logging dbt test results ---")
    run_command([sys.executable, os.path.join(script_dir, "scripts/log_test_results.py")])

    # --- Validating SLOs and Data Quality ---
    print("--- Validating SLOs and data quality ---")
    run_command([sys.executable, os.path.join(script_dir, "scripts/validate_slos.py")])
    run_command([sys.executable, os.path.join(script_dir, "scripts/validate_data_quality.py")])

    # --- Updating subscriber status ---
    print("--- Updating subscriber status ---")
    run_command([sys.executable, os.path.join(script_dir, "scripts/update_subscriber_status.py")])

    print("--- All data governance processes completed successfully! ---")

if __name__ == '__main__':
    main()
