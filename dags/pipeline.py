import yfinance as yf
from config import load_configuration
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, min, max
from pyspark.sql import Window
import sqlite3
import logging
logger = logging.getLogger(__name__)

class SparkJob:
    # Singleton pattern
    _instance = None  
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkJob, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized: return
        self.secrets = load_configuration()
        self._spark = None
        self._initialized = True

    def _spark_builder(self):
        builder = (SparkSession.builder
            .appName(self.secrets["name"])
            .config("spark.driver.host", self.secrets["host"])
            .config("spark.driver.bindAddress", self.secrets["host"])
            .config("spark.network.useNetty", self.secrets["netty"])
            .config("spark.network.netty.kqueue.enabled", self.secrets["kqueue"])
            .config("spark.driver.port", self.secrets["port"])
            .config("spark.blockManager.port", self.secrets["block_manager_port"])
            .config("spark.ui.showConsoleProgress", self.secrets["porgress_bar"])
            .config("spark.ui.enabled", self.secrets["ui"])
            .config("spark.sql.extensions", self.secrets["delta_sql_ext"])
            .config("spark.sql.catalog.spark_catalog", self.secrets["delta_sql_catalog"]))
        return builder
    
    def delta_lake_wrapper(self):
        if self._spark is not None: return self._spark
        builder = self._spark_builder()
        self._spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self._spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", self.secrets["delta_check"])
        self._spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", self.secrets["delta_optimizer"])
        self._spark.conf.set("spark.databricks.delta.parallelDelete.enabled", self.secrets["delta_parall_del"])
        self._spark.conf.set("spark.sql.parquet.enableVectorizedReader", self.secrets["delta_vect_read"])
        return self._spark
    
    def get_spark(self):
        return self.delta_lake_wrapper()


class DataPipeLine:
    def __init__(self):
        self._spark_job = SparkJob()
        self._spark = self._spark_job.get_spark()
        self._secrets = load_configuration()

    def _data_fetch(self, ticker_symbol: str, period: str = "max", interval: str = "1d"):
        logger.info(f"Fetching data for {ticker_symbol}...")
        finance_data = yf.download(ticker_symbol, period=period, interval=interval)
        finance_data.reset_index(inplace=True)
        finance_data = finance_data.droplevel(level=1, axis=1)
        logger.info(f"Downloaded {len(finance_data)} rows")
        return self._spark.createDataFrame(finance_data)
    
    def _mirror_to_sqlite(self, spark_df, table_name: str):
        logger.info(f"Mirroring {table_name} to SQLite at {self._secrets['sqlite_path']}")
        try:
            pandas_df = spark_df.toPandas()
            for column, dtype in pandas_df.dtypes.items():
                if str(dtype).startswith("datetime64"):
                    pandas_df[column] = pandas_df[column].astype(str)
            conn = sqlite3.connect(self._secrets["sqlite_path"])
            try:
                pandas_df.to_sql(table_name, conn, if_exists="replace", index=False)
            finally:
                conn.close()
            logger.info(f"Wrote {len(pandas_df)} rows to SQLite table '{table_name}'")
        except Exception as e:
            logger.warning(f"SQLite mirror for '{table_name}' failed: {e}")

    def _bronze_tier(self, spark_data):
        logger.info(f"Writing to bronze tier: {self._secrets['bronze_path']}")
        try:
            spark_data.write.format("delta").mode("append").save(self._secrets["bronze_path"])
        except Exception as e:
            logger.warning(f"Append failed: {e}. Overwriting...")
            spark_data.write.format("delta").mode("overwrite").save(self._secrets["bronze_path"])
        self._mirror_to_sqlite(spark_data, "bronze")
    
    def _silver_tier(self, spark_data):
        logger.info(f"Writing to silver tier: {self._secrets['silver_path']}")
        silver_df = spark_data.select(col("Date").alias("trade_date"), col("Close").alias("price"))
        try:
            silver_df.write.format("delta").mode("append").save(self._secrets["silver_path"])
        except Exception as e:
            logger.warning(f"Append failed: {e}. Overwriting...")
            silver_df.write.format("delta").mode("overwrite").save(self._secrets["silver_path"])
        self._mirror_to_sqlite(silver_df, "silver")
        return silver_df

    def _gold_tier(self, spark_data):
        logger.info(f"Writing to gold tier: {self._secrets['gold_path']}")
        window_7d = Window.orderBy("trade_date").rowsBetween(-6, 0)
        window_30d = Window.orderBy("trade_date").rowsBetween(-29, 0)
        gold_df = (spark_data
            .withColumn("moving_avg_7d", avg("price").over(window_7d))
            .withColumn("moving_avg_30d", avg("price").over(window_30d))
            .withColumn("volatility_30d", stddev("price").over(window_30d))
            .withColumn("high_30d", max("price").over(window_30d))
            .withColumn("low_30d", min("price").over(window_30d))
        )
        try:
            gold_df.write.format("delta").mode("append").save(self._secrets["gold_path"])
        except Exception as e:
            logger.warning(f"Append failed: {e}. Overwriting...")
            gold_df.write.format("delta").mode("overwrite").save(self._secrets["gold_path"])
        self._mirror_to_sqlite(gold_df, "gold")
        return gold_df
