#!/usr/bin/env python3
"""
CLI entry point for explore-raw-logs command.
Explores and analyzes raw logs from the Ingestion.RawLogs Kafka topic.
"""
import subprocess
import sys
from pathlib import Path

def main():
    # Get the path to the scripts directory relative to the CLI root
    cli_root = Path(__file__).parent.parent
    script_path = cli_root / "scripts" / "explore_raw_logs.py"
    subprocess.run([sys.executable, str(script_path)] + sys.argv[1:])

if __name__ == "__main__":
    main() 