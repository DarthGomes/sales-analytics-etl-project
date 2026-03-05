from src.spark_session import get_spark
from src.utils.logger import get_logger

logger = get_logger(__name__)

spark = get_spark()


def create_schemas(spark, schema_list: list) -> None:

    try:
        for schema in schema_list:
            logger.info(f"Creating schema if not exists: {schema}")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        logger.info("Schemas validated successfully")

    except Exception:
        logger.exception("Error while creating schemas")
        raise


