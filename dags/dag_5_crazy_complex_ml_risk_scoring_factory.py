import os
from pathlib import Path
from dagfactory import load_yaml_dags

# Get the base directory dynamically
airflow_home = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
# Points to 'include/crazy_complex_ml_risk_scoring.yml'
# (Assuming your YAML is in the include folder)
config_path = Path(airflow_home) / "include" / "dagfactory_configs" / "crazy_complex_ml_risk_scoring.yml"

if config_path.exists():
    load_yaml_dags(
        globals_dict=globals(),
        config_filepath=str(config_path),
    )
else:
    # This print will show up in the Airflow Scheduler logs on Astro
    print(f"ERROR: Configuration file not found at {config_path}")
