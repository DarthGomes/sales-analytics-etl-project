import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType
from src.spark_session import get_spark
from src.utils.logger import get_logger
from src.utils.table_writer import save_table

spark = get_spark()
logger = get_logger(__name__)

df_sales_order_detail = spark.table('raw.raw_sales_order_detail')

def build_store_sales_order_detail(df_sales_order_detail: DataFrame) -> DataFrame:

    df_casted = (
        df_sales_order_detail
        .withColumn("UnitPrice", F.col("UnitPrice").cast(DecimalType(18,2)))
        .withColumn("UnitPriceDiscount", F.col("UnitPriceDiscount").cast(DecimalType(5,4)))
    )

    return df_casted

df_sales_order_detail_cleaned = df_sales_order_detail.transform(build_store_sales_order_detail)

save_table(
    df_sales_order_detail_cleaned,
    schema="store",
    table_name="store_sales_order_detail"
)