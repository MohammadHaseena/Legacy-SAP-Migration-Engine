import pyspark.sql.functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("step", "")
dbutils.widgets.text("run_date", "")
step = dbutils.widgets.get("step")
run_date = dbutils.widgets.get("run_date")
assert step in ("bronze", "silver", "gold"), f"bad step: {step}"
assert run_date, "run_date is required"

BASE = "/Volumes/workspace/default/sap_data/generated"


def write_run(df, table):
    """Bronze keeps raw history per run date; rerunning the same date replaces it."""
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_run_date = '{run_date}'")
       .saveAsTable(table))


def upsert(df, table, key="SALES_DOC"):
    """Insert new orders, update existing ones. Reprocessing never creates duplicates."""
    if spark.catalog.tableExists(table):
        (DeltaTable.forName(spark, table).alias("t")
            .merge(df.alias("s"), f"t.{key} = s.{key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    else:
        df.write.format("delta").saveAsTable(table)


# ---------------- BRONZE ----------------
if step == "bronze":
    df = (spark.read.option("header", "true")
          .option("inferSchema", "false")
          .csv(f"{BASE}/sales_orders")
          .withColumn("_run_date", F.lit(run_date))
          .withColumn("_ingested_at", F.current_timestamp()))
    write_run(df, "bronze_sales_orders")
    print("bronze rows:", spark.table("bronze_sales_orders").filter(F.col("_run_date") == run_date).count())

# ---------------- SILVER ----------------
elif step == "silver":
    bronze = spark.table("bronze_sales_orders").filter(F.col("_run_date") == run_date)

    typed = (bronze.dropDuplicates(["SALES_DOC"])      # business key: one row per order
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

    n_in = typed.count()
    if n_in == 0:
        raise ValueError(f"No bronze rows for {run_date}")

    upsert(good, "silver_sales_orders")
    upsert(bad, "quarantine_error_log")

    # Checks on the whole tables, across all run dates
    silver_t = spark.table("silver_sales_orders")
    quar_t = spark.table("quarantine_error_log")
    n_silver = silver_t.count()
    n_quar = quar_t.count()
    n_docs = spark.table("bronze_sales_orders").select("SALES_DOC").distinct().count()
    print(f"In (this run): {n_in} | Silver total: {n_silver} | Quarantine total: {n_quar} | Distinct orders in bronze: {n_docs}")

    if n_silver != silver_t.select("SALES_DOC").distinct().count():
        raise ValueError("Duplicate orders in silver!")
    if n_silver + n_quar != n_docs:
        raise ValueError("Rows lost between bronze and silver/quarantine!")

# ---------------- GOLD ----------------
elif step == "gold":
    customers = (spark.read.option("header", "true")
                 .csv(f"{BASE}/customer_master")
                 .dropDuplicates(["KUNNR"]))

    enriched = (spark.table("silver_sales_orders")
                .join(F.broadcast(customers), "KUNNR", "left")
                .withColumn("REGION", F.coalesce("REGION", F.lit("UNKNOWN"))))

    summary = enriched.groupBy("REGION").agg(
        F.round(F.sum("NET_PRICE"), 2).alias("TOTAL_REVENUE"),
        F.count("SALES_DOC").alias("TOTAL_TRANSACTIONS"))

    summary.write.format("delta").mode("overwrite").saveAsTable("gold_sales_summary")
    display(summary)
