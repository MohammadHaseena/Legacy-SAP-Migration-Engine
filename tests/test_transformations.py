from datetime import date
from decimal import Decimal

from src.transformations import (
    parse_dates, cast_price, tag_errors, split_valid_invalid,
)

COLS = "SALES_DOC string, MATERIAL string, KUNNR string, CREATED_ON string, NET_PRICE string"


def make_df(spark, rows):
    return spark.createDataFrame(rows, COLS)


def prepare(spark, rows):
    """Run the same three steps as the pipeline."""
    return tag_errors(cast_price(parse_dates(make_df(spark, rows))))


def by_doc(df, column):
    return {r["SALES_DOC"]: r[column] for r in df.collect()}


# One row per case; the comment says what the case checks
ERROR_ROWS = [
    ("1", "MAT1", "K1", "05/01/2025", "10.00"),    # valid row
    ("2", None,   "K1", "05/01/2025", "10.00"),    # material is NULL
    ("3", "",     "K1", "05/01/2025", "10.00"),    # material is empty text
    ("4", "MAT1", "K1", "2025-01-05", None),       # price missing
    ("5", "MAT1", "K1", "2025-01-05", "-5.00"),    # price negative
    ("6", "MAT1", "K1", "99/99/9999", "10.00"),    # date cannot be parsed
    ("7", "",     "K1", "05/01/2025", "-5.00"),    # two problems: material wins (priority)
    ("8", "MAT1", "K1", "05/01/2025", "abc"),      # price is not a number
]


def test_parse_dates_handles_all_formats(spark):
    rows = [
        ("1", "M", "K", "05/01/2025", "1"),
        ("2", "M", "K", "2025-01-05", "1"),
        ("3", "M", "K", "20250105",   "1"),
        ("4", "M", "K", "05.01.2025", "1"),
        ("5", "M", "K", "99/99/9999", "1"),
        ("6", "M", "K", None,         "1"),
    ]
    result = by_doc(parse_dates(make_df(spark, rows)), "CREATED_ON")
    for doc in ("1", "2", "3", "4"):
        assert result[doc] == date(2025, 1, 5)
    assert result["5"] is None   # garbage date becomes NULL, not an error
    assert result["6"] is None


def test_cast_price_turns_bad_text_into_null(spark):
    rows = [
        ("1", "M", "K", "05/01/2025", "12.50"),
        ("2", "M", "K", "05/01/2025", "-3"),
        ("3", "M", "K", "05/01/2025", "abc"),
        ("4", "M", "K", "05/01/2025", None),
    ]
    result = by_doc(cast_price(make_df(spark, rows)), "NET_PRICE")
    assert result["1"] == Decimal("12.50")
    assert result["2"] == Decimal("-3.00")
    assert result["3"] is None
    assert result["4"] is None


def test_tag_errors_gives_expected_reason_per_row(spark):
    result = by_doc(prepare(spark, ERROR_ROWS), "ERROR_REASON")
    assert result == {
        "1": None,
        "2": "MISSING_MATERIAL",
        "3": "MISSING_MATERIAL",
        "4": "NULL_OR_INVALID_NET_PRICE",
        "5": "NEGATIVE_NET_PRICE",
        "6": "UNPARSEABLE_DATE",
        "7": "MISSING_MATERIAL",
        "8": "NULL_OR_INVALID_NET_PRICE",
    }


def test_split_loses_no_rows(spark):
    tagged = prepare(spark, ERROR_ROWS)
    good, bad = split_valid_invalid(tagged)
    assert good.count() + bad.count() == tagged.count()
    assert good.count() == 1                      # only row "1" is valid
    assert "ERROR_REASON" not in good.columns
    assert "ERROR_REASON" in bad.columns
