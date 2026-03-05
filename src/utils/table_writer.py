import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from src.utils.logger import get_logger

logger = get_logger(__name__)


def save_table(
    df: DataFrame,
    schema: str,
    table_name: str,
    mode: str = "overwrite"
) -> None:

    full_table_name = f"{schema}.{table_name}"

    try:
        logger.info(f"Starting run: {full_table_name}")

        df_with_timestamp = df.withColumn(
            "loaded_at",
            F.current_timestamp()
        )

        (
            df_with_timestamp.write
            .format("delta")
            .mode(mode)
            .option("path", f"/app/data/lake/{schema}/{table_name}")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )

        logger.info(f"{full_table_name} saved successfully")

    except Exception:
        logger.exception(f"Error saving table {full_table_name}")
        raise