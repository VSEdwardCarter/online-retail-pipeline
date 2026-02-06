from pyspark.sql import functions as F
from src.spark import spark_builder
from src.paths import RAW_ONLINE, BRONZE_DIR, s

RAW_INPUT_FILE = RAW_ONLINE / "Online_Retail.csv"
BRONZE_OUTPUT_FILE = BRONZE_DIR / "onlineRetailBronze"

def main():
    spark = spark_builder("online_retail_bronze_ingest")

    df = (
        spark.read
        .option("header", "true")
        .option("multiline", "false")
        .option("escape", "\"")
        .csv(s(RAW_INPUT_FILE))
    )

    df = df.withColumn("_source_file", F.input_file_name()).withColumn("_ingest_date", F.current_timestamp())

    (
        df.write
        .mode("overwrite")
        .parquet(s(BRONZE_OUTPUT_FILE))
    )

    spark.stop()

if __name__ == "__main__":
    main()