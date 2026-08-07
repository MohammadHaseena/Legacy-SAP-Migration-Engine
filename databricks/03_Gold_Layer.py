# ==========================================
# Gold Layer
# Business Aggregation
# ==========================================

from pyspark.sql.functions import broadcast, sum, count

# Read Customer Master

customer_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/default/sap_data/sap_customer_master.csv")

# Broadcast Join

gold_df = silver_df.join(
    broadcast(customer_df),
    on="KUNNR",
    how="left"
)

# Business Summary

gold_summary_df = gold_df.groupBy("REGION").agg(
    sum("NET_PRICE").alias("TOTAL_REVENUE"),
    count("SALES_DOC").alias("TOTAL_TRANSACTIONS")
)

# Save Gold Table

gold_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_sales_summary")

# Optimize

spark.sql("""
OPTIMIZE gold_sales_summary
ZORDER BY (REGION)
""")

# Verify

display(spark.sql("SELECT * FROM gold_sales_summary"))