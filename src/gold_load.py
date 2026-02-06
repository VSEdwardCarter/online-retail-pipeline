from pyspark.sql import functions as F
from src.spark import spark_builder
from src.paths import SILVER_DIR, GOLD_DIR, s
from silver_extract import SILVER_OUTPUT_FILE

GOLD_OUTPUT_FILE = GOLD_DIR / "gold_sales_analytics"

def main():
    spark = spark_builder("online_retail_gold")

    goldDF = (
        spark.read.parquet(s(SILVER_OUTPUT_FILE))
    )


#     gold_sales_analytics
# ├── invoice_no            (string)
# ├── invoice_ts            (timestamp)
# ├── invoice_date          (date)
#
# ├── customer_id           (string)
# ├── stock_code            (string)
# ├── description           (string)
# ├── country               (string)
#
# ├── quantity              (int)
# ├── unit_price            (double)
# ├── gross_amount          (double)
#
# ├── is_return             (boolean)
# ├── order_type            (string)
#
# ├── ingest_ts             (timestamp)
# ├── ingest_date           (date)


    clean = (
        goldDF
        .withColumn("invoice_no", F.col("InvoiceNo"))
        .withColumn("invoice_ts", F.to_timestamp("InvoiceDate"))
        .withColumn("invoice_date", F.col("InvoiceDate"))
        .withColumn("customer_id", F.col("CustomerID"))
        .withColumn("stock_code", F.col("StockCode").cast("string"))
        .withColumn("description", F.col("Description"))
        .withColumn("country", F.col("Country"))
        .withColumn("quantity", F.col("Quantity"))
        .withColumn("unit_price", F.col("UnitPrice"))
        .withColumn("gross_amount", F.col("unit_price")*F.col("quantity"))
        .withColumn("is_return", F.col("quantity")<1)
        .withColumn(
            "order_type",
            F.when(F.col("Quantity") < 0, F.lit("return"))
            .otherwise(F.lit("sale"))
        )
        .withColumn("ingest_ts", F.col("_ingest_date"))
        .withColumn("ingest_date", F.to_date("_ingest_date"))



    )
    (
        clean
        .coalesce(1)
        .write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(s(GOLD_OUTPUT_FILE))
    )

    spark.stop()

if __name__ == "__main__":
    main()





