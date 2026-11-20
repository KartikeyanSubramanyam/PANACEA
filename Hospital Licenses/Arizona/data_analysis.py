#!/usr/bin/env python3
"""
List the unique values in the TYPE, SUBTYPE, LICENSE_TYPE, and
LICENSE_SUBTYPE columns of a CSV file.

Usage:
    python list_types.py path/to/file.csv
"""

import csv
import sys

COLUMNS = ["TYPE", "SUBTYPE", "LICENSE_TYPE", "LICENSE_SUBTYPE"]


def main(path):
    unique = {col: set() for col in COLUMNS}

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        missing = [c for c in COLUMNS if c not in (reader.fieldnames or [])]
        if missing:
            sys.exit(f"Error: missing column(s) in CSV: {', '.join(missing)}")

        for row in reader:
            for col in COLUMNS:
                value = (row[col] or "").strip()
                if value:
                    unique[col].add(value)

    for col in COLUMNS:
        print(f"\n{col} ({len(unique[col])} unique):")
        for value in sorted(unique[col]):
            print(f"  - {value}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python list_types.py path/to/file.csv")
    main(sys.argv[1])