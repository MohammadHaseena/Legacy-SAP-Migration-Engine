# ==========================================
# Silver Layer
# Data Cleansing & Quality Checks
# ==========================================

from pyspark.sql.functions import col, when, to_date

# Read Bronze Table

bronze_df = spark.table("bronze_sales_orders")

# Remove Duplicates

silver_df = bronze_df.dropDuplicates()

# Remove Null Material

silver_df = silver_df.filter(
    col("MATERIAL").isNotNull() &
    (col("MATERIAL") != "")
)

# Standardize Dates

silver_df = silver_df.withColumn(
    "CREATED_ON",
    when(
        col("CREATED_ON").contains("/"),
        to_date(col("CREATED_ON"), "dd/MM/yyyy")
    ).otherwise(
        to_date(col("CREATED_ON"), "yyyy-MM-dd")
    )
)

# Quarantine Invalid Records

quarantine_df = silver_df.filter(col("NET_PRICE") < 0)

quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("quarantine_error_log")

# Keep Valid Records

silver_df = silver_df.filter(col("NET_PRICE") >= 0)

display(silver_df.limit(10))