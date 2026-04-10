import os
from pathlib import Path
from dagfactory import load_yaml_dags

# =============================================================================
# DAG 5: Crazy Complex ML Risk Scoring Factory (Lab Requirement #4)
# =============================================================================
# Purpose: Maximum Graph View complexity using dagfactory + YAML config
# This is the "wow" DAG for the demo


"""
Ultra-complex DAG generated via dagfactory.
The YAML in include/dagfactory_configs/crazy_complex_ml_risk_scoring.yml
creates dozens of tasks, branching, TaskGroups, and cross-dependencies
to produce the most visually complex Graph View possible.
"""

airflow_home = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
config_path = Path(airflow_home) / "include" / "dagfactory_configs" / "crazy_complex_ml_risk_scoring.yml"

if config_path.exists():
    load_yaml_dags(
        globals_dict=globals(),
        config_filepath=str(config_path),
    )
else:
    print(f"ERROR: Configuration file not found at {config_path}")