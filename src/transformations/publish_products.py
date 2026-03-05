import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from src.spark_session import get_spark
from src.utils.logger import get_logger
from src.utils.table_writer import save_table

spark = get_spark()
logger = get_logger(__name__)

products_df = spark.table("store.store_products")

def create_publish_products(df: DataFrame) -> DataFrame:
    publish_products = (
        df
        .withColumn(
            "Color",
            F.when(F.col("Color").isNull(), F.lit("N/A"))
            .otherwise(F.col("Color"))
        )
        .withColumn(
            "ProductCategoryName",
            F.when(
                (F.col("ProductCategoryName").isNull()) &
                (F.col("ProductSubCategoryName").isin(
                    "Gloves", "Shorts", "Socks", "Tights", "Vests"
                )),
                F.lit("Clothing")
            )
            .when(
                (F.col("ProductCategoryName").isNull()) &
                (F.col("ProductSubCategoryName").isin(
                    "Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"
                )),
                F.lit("Accessories")
            )
            .when(
                (F.col("ProductCategoryName").isNull()) &
                (
                    F.col("ProductSubCategoryName").contains("Frames") |
                    F.col("ProductSubCategoryName").isin("Wheels", "Saddles")
                ),
                F.lit("Components")
            )
            .otherwise(F.col("ProductCategoryName"))
        )
    )

    return publish_products

df_publish_products = products_df.transform(create_publish_products)

save_table(
    df=df_publish_products,
    schema="publish",
    table_name="publish_products"
)