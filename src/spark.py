from pyspark.sql import SparkSession

def spark_builder(app_name:str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    return spark