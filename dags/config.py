import os
from dotenv import load_dotenv

BASE_DIR = '.'  # Separate from Airflow home

def load_configuration() -> dict:
    load_dotenv()
    os.makedirs(os.path.join(BASE_DIR, "data_lake"), exist_ok=True)
    bronze_path = os.path.join(BASE_DIR, "data_lake/bronze")
    silver_path = os.path.join(BASE_DIR, "data_lake/silver")
    gold_path = os.path.join(BASE_DIR, "data_lake/gold")
    for path in [bronze_path, silver_path, gold_path]: os.makedirs(path, exist_ok=True)
    sqlite_path = os.path.join(BASE_DIR, "data_lake", "pipeline.db")
    secrets = {
        "name": os.getenv("APP_NAME", "stock_pipeline"),
        "host": os.getenv("SPARK_HOST", "127.0.0.1"),
        "netty": os.getenv("SPARK_NETTY", "true"),
        "kqueue": os.getenv("SPARK_KQUEUE", "false"),  # false on Intel Macs
        "port": os.getenv("SPARK_DRIVER_PORT", "54321"),
        "block_manager_port": os.getenv("SPARK_BLOCK_MNGR_PORT", "54322"),
        "porgress_bar": os.getenv("SPARK_SUPRESS_PROGRESS_BAR", "false"),
        "ui": os.getenv("SPARK_ENABLE_UI", "false"),  # Disable UI locally
        "recovery_mode": os.getenv("SPARK_RECOVERY_MODE", "NONE"),
        "ssl_enabled": os.getenv("SPARK_SSL_ENABLED", "false"),
        "ssl_key_pass": os.getenv("SPARK_SSL_KEY_PASS", ""),
        "ssl_key_store": os.getenv("SPARK_SSL_KEY_STORE", ""),
        "ssl_key_store_pass": os.getenv("SPARK_SSL_KEY_STORE_PASS", ""),
        "ssl_trust_store": os.getenv("SPARK_SSL_TRUST_STORE", ""),
        "ssl_trust_store_pass": os.getenv("SPARK_SSL_TRUST_STORE_PASS", ""),
        "auth": os.getenv("SPARK_AUTH", "false"),
        "auth_secret": os.getenv("SPARK_AUTH_SECRET", ""),
        "exec_memory": os.getenv("SPARK_EXEC_MEMORY", "2g"),
        "exec_cores": os.getenv("SPARK_EXEC_CORES", "2"),
        "max_cores": os.getenv("SPARK_MAX_CORES", "4"),
        "delta_sql_ext": os.getenv("DELTA_SQL_EXTENSION", "io.delta.sql.DeltaSparkSessionExtension"),
        "delta_sql_catalog": os.getenv("DELTA_SQL_CATALOG", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        "delta_check": os.getenv("DELTA_CHECK", "false"),
        "delta_optimizer": os.getenv("DELTA_OPTIMIZER", "true"),
        "delta_parall_del": os.getenv("DELTA_PARALLEL_DELETE", "true"),
        "delta_vect_read": os.getenv("DELTA_VECT_READER", "true"),
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "gold_path": gold_path,
        "sqlite_path": sqlite_path,
    }
    return secrets
