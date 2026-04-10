import os
from pathlib import Path
from dagfactory import load_yaml_dags

DEFAULT_CONFIG_ROOT_DIR = "/usr/local/airflow/dags/"
CONFIG_ROOT_DIR = Path(os.getenv("CONFIG_ROOT_DIR", DEFAULT_CONFIG_ROOT_DIR))

config_file = str(CONFIG_ROOT_DIR / "crazy_complex_ml_risk_scoring.yml")

load_yaml_dags(
    globals_dict=globals(),
    config_filepath=config_file,
)