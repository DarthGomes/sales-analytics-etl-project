import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from src.spark_session import get_spark
from src.utils.logger import get_logger
from src.utils.table_writer import save_table

spark = get_spark()
logger = get_logger(__name__)

def build_publish_orders(
    detail_df: DataFrame,
    header_df: DataFrame
) -> DataFrame:

    df = (
        detail_df.alias("d")
        .join(
            header_df.alias("h"),
            F.col("d.SalesOrderID") == F.col("h.SalesOrderID"),
            "inner"
        )
    )

    df = df.withColumn(
        "LeadTimeInBusinessDays",
        F.size(
            F.filter(
                F.sequence(
                    F.to_date("OrderDate"),
                    F.to_date("ShipDate")
                ),
                lambda x: F.dayofweek(x).between(2, 6)
            )
        )
    )

    df = df.withColumn(
        "TotalLineExtendedPrice",
        F.col("OrderQty") * (F.col("UnitPrice") - F.col("UnitPriceDiscount"))
    )

    df = df.withColumnRenamed(
        "Freight",
        "TotalOrderFreight"
    )

    df = df.select(
        "d.*",
        "OrderDate",
        "ShipDate",
        "OnlineOrderFlag",
        "AccountNumber",
        "CustomerID",
        "SalesPersonID",
        "TotalOrderFreight",
        "LeadTimeInBusinessDays",
        "TotalLineExtendedPrice"
    )

    return df



detail_df = spark.table("store.store_sales_order_detail").drop('loaded_at')
header_df = spark.table("store.store_sales_order_header").drop('loaded_at')

publish_orders_df = build_publish_orders(detail_df, header_df)


save_table(
    publish_orders_df,
    schema="publish",
    table_name="publish_orders")

  