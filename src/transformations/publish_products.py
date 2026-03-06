import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from src.spark_session import get_spark
from src.utils.logger import get_logger
from src.utils.table_writer import save_table

spark = get_spark()
logger = get_logger(__name__)

products_df = spark.table("store.store_products")


def create_publish_products(df: DataFrame) -> DataFrame:

    logger.info("Starting publish_products transformation")

    clothing = [
        "Gloves", "Shorts", "Socks", "Tights", "Vests",
        "Caps", "Bib-Shorts"
    ]

    accessories = [
        "Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps",
        "Hydration Packs", "Bottles and Cages", "Panniers",
        "Bike Racks", "Fenders", "Bike Stands", "Cleaners"
    ]

    components = [
        "Wheels", "Saddles",
        "Bottom Brackets", "Derailleurs", "Chains",
        "Forks", "Brakes", "Cranksets", "Tires and Tubes"
    ]

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
                (F.col("ProductSubCategoryName").isin(clothing)),
                F.lit("Clothing")
            )
            .when(
                (F.col("ProductCategoryName").isNull()) &
                (F.col("ProductSubCategoryName").isin(accessories)),
                F.lit("Accessories")
            )
            .when(
                (F.col("ProductCategoryName").isNull()) &
                (
                    F.col("ProductSubCategoryName").contains("Frames") |
                    F.col("ProductSubCategoryName").isin(components)
                ),
                F.lit("Components")
            )
            .otherwise(F.col("ProductCategoryName"))
        )
    )

    logger.info("publish_products transformation finished")

    return publish_products


df_publish_products = products_df.transform(create_publish_products)

logger.info("Saving publish_products table")

save_table(
    df=df_publish_products,
    schema="publish",
    table_name="publish_products"
)

logger.info("publish_products saved successfully")