from pyspark.sql import functions as F
from src.spark import spark_builder
from src.paths import BRONZE_DIR, SILVER_DIR, s

BRONZE_INPUT_FILE = BRONZE_DIR/"onlineRetailBronze"
SILVER_OUTPUT_FILE = SILVER_DIR /"onlineRetailSilver"

def main():
    spark = spark_builder("online_retail_silver_extract")

    silver_df = (
        spark.read.parquet(s(BRONZE_INPUT_FILE))
    )

    # ====== NORMALIZE COLUMNS ======
              # STRING
    # |-- Description: string (nullable = true)
    # |-- CustomerID: string (nullable = true)
    # |-- Country: string (nullable = true)
    # |-- _source_file: string (nullable = true)
              # FLOAT
    # |-- UnitPrice: string (nullable = true)
              # INT
    # |-- InvoiceNo: string (nullable = true)
    # |-- StockCode: string (nullable = true)
    # |-- Quantity: string (nullable = true)
              # TIMESTAMP
    # |-- _ingest_date: timestamp (nullable = true)
    # |-- InvoiceDate: string (nullable = true)

    silver_df = (
        silver_df
        .withColumn("InvoiceNo", F.col("InvoiceNo").cast("int"))
        .withColumn("StockCode", F.col("StockCode").cast("int"))
        .withColumn("Quantity", F.col("Quantity").cast("int"))
        .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
        .withColumn("invoice_ts", F.to_timestamp(F.col("InvoiceDate"), "M/d/yy H:mm"))
        .withColumn("InvoiceDate", F.to_date("invoice_ts"))
        .drop("invoice_ts")
    )

    (
        silver_df.repartition("InvoiceDate")
        .write.mode("overwrite")
        .partitionBy("InvoiceDate")
        .parquet(s(SILVER_OUTPUT_FILE))
    )

    spark.stop()

if __name__ == "__main__":
    main()


