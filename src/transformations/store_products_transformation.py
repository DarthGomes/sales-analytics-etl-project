import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from pyspark.sql.types import DecimalType
from src.spark_session import get_spark
from src.utils.logger import get_logger
from src.utils.table_writer import save_table

logger = get_logger(__name__)


def build_store_product(df: DataFrame) -> DataFrame:

    df_clean = (
        df.groupBy(
            "ProductID",
            "ProductDesc",
            "ProductNumber",
            "MakeFlag",
            "Color",
            "SafetyStockLevel",
            "ReorderPoint",
            "Size",
            "SizeUnitMeasureCode",
            "WeightUnitMeasureCode"
        )
        .agg(
            F.first("StandardCost", ignorenulls=True).alias("StandardCost"),
            F.first("ListPrice", ignorenulls=True).alias("ListPrice"),
            F.first("Weight", ignorenulls=True).alias("Weight"),
            F.first("ProductCategoryName", ignorenulls=True).alias("ProductCategoryName"),
            F.first("ProductSubCategoryName", ignorenulls=True).alias("ProductSubCategoryName")
        )
    )

    df_casted = (
        df_clean
        .withColumn("StandardCost", F.col("StandardCost").cast(DecimalType(18,2)))
        .withColumn("ListPrice", F.col("ListPrice").cast(DecimalType(18,2)))
        .withColumn("Weight", F.col("Weight").cast(DecimalType(18,2)))
    )

    return df_casted


def run_store_products():

    spark = get_spark()

    logger.info("Starting store_products transformation")

    df_products = spark.table("raw.raw_product")

    df_products_cleaned = df_products.transform(build_store_product)

    save_table(
        df=df_products_cleaned,
        schema="store",
        table_name="store_products"
    )

    logger.info("store_products saved successfully")