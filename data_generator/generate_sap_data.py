import pyspark.sql.functions as F

OUT = "/Volumes/workspace/default/sap_data/generated"
N_ORDERS = 1_000_000
N_CUSTOMERS = 5_000
N_MATERIALS = 500

# ---------- CUSTOMER MASTER ----------
regions = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
customers = (spark.range(N_CUSTOMERS)
    .withColumn("KUNNR", F.lpad(F.col("id").cast("string"), 10, "0"))   # leading zeros like SAP
    .withColumn("REGION", F.array(*[F.lit(r) for r in regions])[(F.col("id") % 5).cast("int")])
    .withColumn("CUSTOMER_NAME", F.concat(F.lit("Customer "), F.col("id").cast("string")))
    .select("KUNNR", "CUSTOMER_NAME", "REGION"))

customers.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUT}/customer_master")

# ---------- SALES ORDERS ----------
o = spark.range(N_ORDERS)
o = (o
    .withColumn("SALES_DOC", F.lpad((F.col("id") + 4500000000).cast("string"), 10, "0"))
    .withColumn("_cust", F.floor(F.rand(1) * N_CUSTOMERS))
    .withColumn("_mat", F.floor(F.rand(2) * N_MATERIALS))
    .withColumn("_price", F.round(F.rand(3) * 5000 + 10, 2))
    .withColumn("_date", F.date_add(F.lit("2025-01-01").cast("date"), F.floor(F.rand(4) * 365).cast("int")))
    .withColumn("_r_fmt", F.rand(5))
    .withColumn("_r_price", F.rand(6))
    .withColumn("_r_mat", F.rand(7))
    .withColumn("_r_cust", F.rand(8))
    .withColumn("_r_date", F.rand(9)))

orders = (o
    # customers: 1% are orphans that don't exist in the master
    .withColumn("KUNNR", F.lpad(
        F.when(F.col("_r_cust") < 0.01, F.col("_cust") + N_CUSTOMERS).otherwise(F.col("_cust")).cast("string"), 10, "0"))
    # materials: 1% missing (half null, half empty string)
    .withColumn("MATERIAL",
        F.when(F.col("_r_mat") < 0.005, F.lit(None).cast("string"))
         .when(F.col("_r_mat") < 0.01, F.lit(""))
         .otherwise(F.concat(F.lit("MAT"), F.lpad(F.col("_mat").cast("string"), 6, "0"))))
    # prices: 1% negative, 0.5% missing
    .withColumn("NET_PRICE",
        F.when(F.col("_r_price") < 0.005, F.lit(None).cast("string"))
         .when(F.col("_r_price") < 0.015, (-F.col("_price")).cast("string"))
         .otherwise(F.col("_price").cast("string")))
    # dates: four different formats, plus 0.5% garbage
    .withColumn("CREATED_ON",
        F.when(F.col("_r_date") < 0.005, F.lit("99/99/9999"))
         .when(F.col("_r_fmt") < 0.4, F.date_format("_date", "dd/MM/yyyy"))
         .when(F.col("_r_fmt") < 0.8, F.date_format("_date", "yyyy-MM-dd"))
         .when(F.col("_r_fmt") < 0.9, F.date_format("_date", "yyyyMMdd"))
         .otherwise(F.date_format("_date", "dd.MM.yyyy")))
    .select("SALES_DOC", "MATERIAL", "KUNNR", "CREATED_ON", "NET_PRICE"))

# 2% exact duplicate rows
orders = orders.unionByName(orders.sample(0.02, seed=42))

orders.coalesce(4).write.mode("overwrite").option("header", "true").csv(f"{OUT}/sales_orders")

print("Done. Orders written:", orders.count())
display(dbutils.fs.ls("/Volumes/workspace/default/sap_data/generated/sales_orders"))
