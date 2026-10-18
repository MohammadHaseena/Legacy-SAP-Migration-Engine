import pyspark.sql.functions as F

BASE = "/Volumes/workspace/default/sap_data/generated"

silver = spark.table("silver_sales_orders")
assert silver.count() > 0, "silver_sales_orders is empty, run the pipeline first"

# ---------------- dim_customer: one row per customer ----------------
customers = (spark.read.option("header", "true")
             .csv(f"{BASE}/customer_master")
             .dropDuplicates(["KUNNR"])               # one row per customer, or joins inflate revenue
             .select("KUNNR", "CUSTOMER_NAME", "REGION"))

# extra row so orders with a missing customer still have a place to point
unknown = spark.createDataFrame(
    [("UNKNOWN", "Unknown customer", "UNKNOWN")],
    ["KUNNR", "CUSTOMER_NAME", "REGION"])

dim_customer = customers.unionByName(unknown)
dim_customer.write.format("delta").mode("overwrite").saveAsTable("dim_customer")

# ---------------- dim_material: one row per product ----------------
dim_material = silver.select("MATERIAL").distinct()
dim_material.write.format("delta").mode("overwrite").saveAsTable("dim_material")

# ---------------- dim_date: one row per calendar day ----------------
bounds = silver.agg(F.min("CREATED_ON").alias("d0"),
                    F.max("CREATED_ON").alias("d1")).first()

dim_date = (spark.sql(f"""
        SELECT explode(sequence(to_date('{bounds.d0}'),
                                to_date('{bounds.d1}'),
                                interval 1 day)) AS FULL_DATE""")
    .withColumn("DATE_KEY", F.date_format("FULL_DATE", "yyyyMMdd").cast("int"))
    .withColumn("YEAR", F.year("FULL_DATE"))
    .withColumn("QUARTER", F.quarter("FULL_DATE"))
    .withColumn("MONTH", F.month("FULL_DATE"))
    .withColumn("MONTH_NAME", F.date_format("FULL_DATE", "MMMM"))
    .withColumn("DAY_OF_WEEK", F.date_format("FULL_DATE", "EEEE"))
    .withColumn("IS_WEEKEND", F.dayofweek("FULL_DATE").isin(1, 7)))
dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_date")

# ---------------- fact_sales: one row per order ----------------
known = (spark.table("dim_customer").select("KUNNR")
         .withColumn("_known", F.lit(True)))

fact_sales = (silver
    .join(F.broadcast(known), "KUNNR", "left")
    .withColumn("CUSTOMER_KEY",
                F.when(F.col("_known").isNull(), F.lit("UNKNOWN"))   # orphan orders -> UNKNOWN row
                 .otherwise(F.col("KUNNR")))
    .withColumn("DATE_KEY", F.date_format("CREATED_ON", "yyyyMMdd").cast("int"))
    .select("SALES_DOC", "CUSTOMER_KEY", "MATERIAL", "DATE_KEY", "NET_PRICE", "_run_date"))
fact_sales.write.format("delta").mode("overwrite").saveAsTable("fact_sales")

# ---------------- checks: fail the task if anything is off ----------------
n_silver = silver.count()
n_fact = spark.table("fact_sales").count()
n_unknown =
