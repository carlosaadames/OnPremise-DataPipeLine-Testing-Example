import subprocess
import os
import json
from pathlib import Path
import argparse

def main():
    # Initialize the parser
    parser = argparse.ArgumentParser(description="Process stock configuration.")

    # Define arguments with defaults
    parser.add_argument("--ticker", type=str, default="MSFT", help="The ticker symbol (e.g., MSFT, AAPL)")
    parser.add_argument("--period", type=str, default="max", help="The time period (e.g., 1d, 5d, max)")
    parser.add_argument("--interval", type=str, default="1d", help="The data interval (e.g., 1d, 1h)")

    # Parse command line input
    args = parser.parse_args()
    run_conf = {"ticker": args.ticker, "period": args.period, "interval": args.interval}

    # --- DAG Configuration ---
    PROJECT_ROOT = Path(__file__).parent.resolve()
    LOCAL_AIRFLOW_HOME = PROJECT_ROOT / ".airflow"
    os.environ["AIRFLOW_HOME"] = str(LOCAL_AIRFLOW_HOME)
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(PROJECT_ROOT / "dags")
    
    current_env = os.environ.copy()
    current_env["PYTHONPATH"] = str(PROJECT_ROOT)

    print("Syncing local Airflow metadata database schema...")
    subprocess.run(["airflow", "db", "migrate"], env=current_env, check=True)
 
    print("Checking Airflow configuration:")
    subprocess.run(["airflow", "config", "get-value", "core", "dags_folder"], env=current_env)
 
    print("Registering DAGs by scanning the dags folder...")
    result = subprocess.run(
        ["airflow", "dags", "list"],
        env=current_env, capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
 
    if "spark_pipeline_orchestrator" not in result.stdout:
        print("DAG not found yet. Forcing reserialize before giving up...")
        subprocess.run(["airflow", "dags", "reserialize"], env=current_env, check=True, cwd=str(PROJECT_ROOT))
        result = subprocess.run(
            ["airflow", "dags", "list"],
            env=current_env, capture_output=True, text=True, cwd=str(PROJECT_ROOT)
        )
        print("STDOUT:", result.stdout)
 
    if "spark_pipeline_orchestrator" not in result.stdout:
        print("ERROR: DAG still not found. Checking for import errors...")
        import_errors = subprocess.run(
            ["airflow", "dags", "list-import-errors"],
            env=current_env, capture_output=True, text=True, cwd=str(PROJECT_ROOT)
        )
        print("IMPORT ERRORS STDOUT:", import_errors.stdout)
        if import_errors.stderr:
            print("IMPORT ERRORS STDERR:", import_errors.stderr)
        exit(1)
 
    print("DAG found. Launching execution...")
    subprocess.run(
        ["airflow", "dags", "test", "spark_pipeline_orchestrator", "2026-07-16", "--conf", json.dumps(run_conf)],
        env=current_env, check=True
    )
if __name__ == "__main__":
    main()
