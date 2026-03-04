from spark_session import get_spark
from utils.logger import get_logger

logger = get_logger(__name__)

def run_ingestion():

    spark = get_spark()

    logger.info("Starting ingestion layer (raw)...")

    try:
        # =========================
        # Product
        # =========================
        logger.info("Loading Product.csv")

        product_df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv("/app/data/input/products-1-.csv")

        product_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("path", "/app/data/lake/raw/raw_product") \
            .saveAsTable("raw_product")

        logger.info("raw_product table created successfully.")

        # =========================
        # SalesOrderHeader
        # =========================
        logger.info("Loading SalesOrderHeader.csv")

        header_df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv("/app/data/input/sales-order-header-1-.csv")

        header_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("path", "/app/data/lake/raw/raw_sales_order_header") \
            .saveAsTable("raw_sales_order_header")

        logger.info("raw_sales_order_header table created successfully.")

        # =========================
        # SalesOrderDetail
        # =========================
        logger.info("Loading SalesOrderDetail.csv")

        detail_df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv("/app/data/input/sales-order-detail-1-.csv")

        detail_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("path", "/app/data/lake/raw/raw_sales_order_detail") \
            .saveAsTable("raw_sales_order_detail")

        logger.info("raw_sales_order_detail table created successfully.")

        logger.info("Ingestion layer finished successfully.")

    except Exception as e:
        logger.exception("Error during ingestion process.")
        raise e


if __name__ == "__main__":
    run_ingestion()