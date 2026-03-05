from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum

def get_table(spark, table_name: str) -> DataFrame:
    return spark.table(table_name)


def check_not_null(spark, table_name: str, columns: list):
    df = spark.table(table_name)

    results = {}

    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        results[column] = null_count

    return results

def check_no_duplicates(spark, table_name: str, key_columns: list):
    df = spark.table(table_name)

    duplicates = (
        df.groupBy(key_columns)
          .count()
          .filter("count > 1")
    )

    return duplicates.count()

def check_fk_integrity(
    spark,
    child_table: str,
    parent_table: str,
    join_column: str
):
    child = spark.table(child_table)
    parent = spark.table(parent_table)

    invalid = child.join(parent, join_column, "left_anti")

    return invalid.count()

def check_negative_values(spark, table_name: str, columns: list):
    df = spark.table(table_name)

    results = {}

    for column in columns:
        negative_count = df.filter(col(column) < 0).count()
        results[column] = negative_count

    return results