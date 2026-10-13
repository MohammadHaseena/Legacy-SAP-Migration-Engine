import pyspark.sql.functions as F

run_date = "2026-10-13"   # any date label, used to keep reruns safe
BASE = "/Volumes/workspace/default/sap_data"

def write_run(df, table):
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_run_date = '{run_date}'")
       .saveAsTable(table))

# ---------- BRONZE: raw data, nothing changed ----------
bronze_df = (spark.read.option("header", "true")
             .option("inferSchema", "false")      # keeps leading zeros like 0000100234
             .csv(f"{BASE}/generated/sales_orders")
             .withColumn("_run_date", F.lit(run_date))
             .withColumn("_ingested_at", F.current_timestamp()))
write_run(bronze_df, "bronze_sales_orders")
print("Bronze rows:", bronze_df.count())

# ---------- SILVER: clean data, bad rows go to quarantine ----------
bronze = spark.table("bronze_sales_orders").filter(F.col("_run_date") == run_date)

key = ["SALES_DOC", "MATERIAL", "KUNNR", "CREATED_ON", "NET_PRICE"]  # change to your real unique columns
typed = (bronze.dropDuplicates(key)
    .withColumn("NET_PRICE", F.col("NET_PRICE").cast("decimal(18,2)"))
    .withColumn("CREATED_ON", F.coalesce(
        F.expr("try_to_date(CREATED_ON, 'dd/MM/yyyy')"),
        F.expr("try_to_date(CREATED_ON, 'yyyy-MM-dd')"),
        F.expr("try_to_date(CREATED_ON, 'yyyyMMdd')"),
        F.expr("try_to_date(CREATED_ON, 'dd.MM.yyyy')"))))

reason = (F.when(F.col("MATERIAL").isNull() | (F.trim("MATERIAL") == ""), "MISSING_MATERIAL")
           .when(F.col("NET_PRICE").isNull(), "NULL_OR_INVALID_NET_PRICE")
           .when(F.col("NET_PRICE") < 0, "NEGATIVE_NET_PRICE")
           .when(F.col("CREATED_ON").isNull(), "UNPARSEABLE_DATE"))

tagged = typed.withColumn("ERROR_REASON", reason)
good = tagged.filter("ERROR_REASON IS NULL").drop("ERROR_REASON")
bad = tagged.filter("ERROR_REASON IS NOT NULL")

write_run(good, "silver_sales_orders")
write_run(bad, "quarantine_error_log")

n_in = typed.count()
n_good = spark.table("silver_sales_orders").filter(F.col("_run_date") == run_date).count()
n_bad = spark.table("quarantine_error_log").filter(F.col("_run_date") == run_date).count()
print(f"In: {n_in} | Silver: {n_good} | Quarantine: {n_bad}")
assert n_good + n_bad == n_in, "Rows were lost between layers!"

# ---------- GOLD: summary for reporting ----------
customers = (spark.read.option("header", "true")
             .csv(f"{BASE}/generated/customer_master")
             .dropDuplicates(["KUNNR"]))          # one row per customer

enriched = (spark.table("silver_sales_orders")
            .join(F.broadcast(customers), "KUNNR", "left")
            .withColumn("REGION", F.coalesce("REGION", F.lit("UNKNOWN"))))

summary = enriched.groupBy("REGION").agg(
    F.round(F.sum("NET_PRICE"), 2).alias("TOTAL_REVENUE"),
    F.count("SALES_DOC").alias("TOTAL_TRANSACTIONS"))

summary.write.format("delta").mode("overwrite").saveAsTable("gold_sales_summary")
display(summary)
%sql
SELECT ERROR_REASON, COUNT(*) AS n FROM quarantine_error_log GROUP BY ERROR_REASON ORDER BY n DESC
%sql
SELECT REGION, TOTAL_REVENUE, TOTAL_TRANSACTIONS FROM gold_sales_summary
