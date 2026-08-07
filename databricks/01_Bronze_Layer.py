# ==========================================
# Bronze Layer
# Read Raw SAP Sales Data
# Store as Delta Table
# ==========================================

# Read CSV from Unity Catalog Volume

sales_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/default/sap_data/sap_sales_orders.csv")

# View data

display(sales_df)

# Check schema

sales_df.printSchema()

# Save as Bronze Delta Table

sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_sales_orders")

# Verify

display(spark.sql("SELECT * FROM bronze_sales_orders LIMIT 10"))