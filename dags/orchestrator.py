from airflow.sdk import dag, task
from datetime import datetime

@dag(dag_id="spark_pipeline_orchestrator", start_date=datetime(2026, 1, 1), schedule=None, catchup=False, params={"ticker":"AAPL","period":"max","interval":"1d"})
def spark_orchestrator():
    @task
    def extract_task(params=None):
        import pipeline
        _pipeline = pipeline.DataPipeLine()
        ticker = params.get("ticker")
        period = params.get("period")
        interval = params.get("interval")
        spark_df = _pipeline._data_fetch(ticker, period, interval)
        _pipeline._bronze_tier(spark_df)
        return "bronze_complete" # Pass state, not data

    @task
    def transform_silver_task(bronze_status: str):
        import pipeline
        _pipeline = pipeline.DataPipeLine()
        spark = _pipeline._spark
        df = spark.read.format("delta").load(_pipeline._secrets["bronze_path"])
        _pipeline._silver_tier(df)
        return "silver_complete"

    @task
    def transform_gold_task(silver_status: str):
        import pipeline
        _pipeline = pipeline.DataPipeLine()
        spark = _pipeline._spark
        df = spark.read.format("delta").load(_pipeline._secrets["silver_path"])
        _pipeline._gold_tier(df)
        return "gold_complete"

    bronze = extract_task()
    silver = transform_silver_task(bronze)
    gold = transform_gold_task(silver)

dag_instance = spark_orchestrator()