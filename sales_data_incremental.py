from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os

# -------------------------------
# Spark Session with Delta
# -------------------------------
builder = (
    SparkSession.builder
    .appName("Sales Incremental Load")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# -------------------------------
# Paths
# -------------------------------
delta_path = "E:/practice/spark/output/sales_data"
csv_file_path = "E:/practice/inputfile/Data/retail_sales_dataset3.csv"

# -------------------------------
# Read incoming CSV data
# -------------------------------
updates_df = spark.read.csv(
    csv_file_path,
    header=True,
    inferSchema=True
)

# Optional: show schema
updates_df.printSchema()

# -------------------------------
# Create Delta table if it does not exist
# -------------------------------
if not os.path.exists(delta_path) or not DeltaTable.isDeltaTable(spark, delta_path):
    print("Delta table does not exist. Creating a new Delta table...")
    
    updates_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .partitionBy("gender") \
        .save(delta_path)

else:
    print("Delta table exists. Performing MERGE (upsert)...")

    delta_table = DeltaTable.forPath(spark, delta_path)
    
    delta_table.alias("t") \
        .merge(
            updates_df.alias("s"),
            "t.transaction_id = s.transaction_id"  # Matching key
        ) \
        .whenMatchedUpdate(set={
            "date": col("s.date"),
            "customer_id": col("s.customer_id"),
            "gender": col("s.gender"),
            "age": col("s.age"),
            "product_category": col("s.product_category"),
            "quantity": col("s.quantity"),
            "price_per_unit": col("s.price_per_unit"),
            "total_amount": col("s.total_amount")
        }) \
        .whenNotMatchedInsertAll() \
        .execute()

# -------------------------------
# Read Delta table as Parquet (optional)
# -------------------------------
    print("Reading Delta table as a DataFrame...")
    print("Optimizing Delta table with Z-Ordering on 'date' column...")
    spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY (transaction_id)")

df = spark.read.format("delta").load(delta_path)
df.show(5)

# You can also write it as Parquet if needed
parquet_path = "E:/practice/spark/output/sales_data_parquet"
df.write.mode("overwrite") \
     .option("mergeSchema", "true") \
     .parquet(parquet_path)
print(f"Delta table also saved as Parquet at {parquet_path}")

# -------------------------------
# Stop Spark session
# -------------------------------
spark.stop()
