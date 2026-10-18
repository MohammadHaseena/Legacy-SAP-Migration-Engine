import pyspark.sql.functions as F

# Date formats seen in the SAP files, tried in this order
DATE_FORMATS = ["dd/MM/yyyy", "yyyy-MM-dd", "yyyyMMdd", "dd.MM.yyyy"]


def parse_dates(df, col="CREATED_ON"):
    """Try each date format in turn; unparseable values become NULL."""
    attempts = [
        F.expr(f"CAST(try_to_timestamp({col}, '{fmt}') AS DATE)")
        for fmt in DATE_FORMATS
    ]
    return df.withColumn(col, F.coalesce(*attempts))


def cast_price(df, col="NET_PRICE"):
    """Cast price text to decimal; invalid text becomes NULL."""
    return df.withColumn(col, F.expr(f"try_cast({col} AS DECIMAL(18,2))"))


def tag_errors(df):
    """Add ERROR_REASON. Rules are checked in priority order; first match wins.
    Rows with no problem get NULL."""
    reason = (
        F.when(F.col("MATERIAL").isNull() | (F.trim("MATERIAL") == ""), "MISSING_MATERIAL")
         .when(F.col("NET_PRICE").isNull(), "NULL_OR_INVALID_NET_PRICE")
         .when(F.col("NET_PRICE") < 0, "NEGATIVE_NET_PRICE")
         .when(F.col("CREATED_ON").isNull(), "UNPARSEABLE_DATE")
    )
    return df.withColumn("ERROR_REASON", reason)


def split_valid_invalid(df):
    """Return (good_rows, bad_rows). Every input row lands in exactly one of them."""
    good = df.filter("ERROR_REASON IS NULL").drop("ERROR_REASON")
    bad = df.filter("ERROR_REASON IS NOT NULL")
    return good, bad
