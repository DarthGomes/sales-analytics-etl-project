from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark():

    builder = (
        SparkSession.builder
        .appName("SalesAnalyticsProject")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/app/spark-warehouse")
        .config(
            "javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=/app/metastore_db;create=true"
        )
        .enableHiveSupport()
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark