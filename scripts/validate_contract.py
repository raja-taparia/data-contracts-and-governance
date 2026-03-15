"""Validate a contract YAML file.

This script is a lightweight sanity-checker for contract YAML files.
It is intended to be used before publishing contracts to a schema registry.

Usage:
    python scripts/validate_contract.py --contract contracts/finance/accounts/v1.yaml

If you want to integrate this into CI, run it as a step and fail when the contract is invalid.
"""

import argparse
import sys

import yaml


REQUIRED_TOP_LEVEL = ["version", "name", "domain", "schema"]


def load_contract(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_contract(contract: dict) -> list[str]:
    errors: list[str] = []

    for key in REQUIRED_TOP_LEVEL:
        if key not in contract:
            errors.append(f"Missing required top-level key: '{key}'")

    schema = contract.get("schema")
    if not isinstance(schema, dict):
        errors.append("'schema' must be a mapping/object")
    else:
        props = schema.get("properties")
        if not isinstance(props, dict):
            errors.append("'schema.properties' must be a mapping/object")
        else:
            for col, col_def in props.items():
                if not isinstance(col_def, dict):
                    errors.append(f"Column '{col}' definition must be an object")
                    continue

                if "type" not in col_def:
                    errors.append(f"Column '{col}' missing required 'type'")

    return errors


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Validate a contract YAML file")
    parser.add_argument("--contract", required=True, help="Path to the contract YAML file")

    args = parser.parse_args(argv)

    contract = load_contract(args.contract)
    errors = validate_contract(contract)

    if errors:
        print("Contract validation failed:")
        for e in errors:
            print(f"  - {e}")
        return 1

    print(f"Contract {args.contract} is valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
