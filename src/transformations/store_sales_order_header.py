import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType
from src.spark_session import get_spark
from src.utils.logger import get_logger
from src.utils.table_writer import save_table

spark = get_spark()
logger = get_logger(__name__)

def run_store_sales_order_header():

    logger.info("Starting store_sales_order_header transformation")

    df = spark.table("raw.raw_sales_order_header")

    df_cleaned = df.withColumn(
        "Freight",
        F.col("Freight").cast(DecimalType(18,2))
    )

    save_table(
        df_cleaned,
        schema="store",
        table_name="store_sales_order_header"
    )

    logger.info("store_sales_order_header saved successfully")