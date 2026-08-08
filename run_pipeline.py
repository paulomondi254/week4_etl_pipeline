"""
Simple ETL pipeline for daily sales data.
Extract from CSV -> transform/clean -> validate -> load into SQLite.
Run with: python run_pipeline.py
"""

import os
import sys
import json
import sqlite3
import logging
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw/sales_clean.csv")
DB_PATH = os.getenv("DB_PATH", "data/processed/sales.db")
MIN_UNIT_PRICE = float(os.getenv("MIN_UNIT_PRICE", 0))
MAX_DISCOUNT_PCT = float(os.getenv("MAX_DISCOUNT_PCT", 50))
MIN_QUANTITY = float(os.getenv("MIN_QUANTITY", 1))
LOG_PATH = os.getenv("LOG_PATH", "pipeline.log")
SUITE_PATH = os.path.join("great_expectations", "sales_suite.json")

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("etl")


def extract(path):
    logger.info(f"Extract started, reading {path}")
    df = pd.read_csv(path)
    logger.info(f"Extract finished, {len(df)} rows read")
    return df


def transform(df):
    logger.info("Transform started")
    before = len(df)

    df = df.copy()
    df["customer"] = df["customer"].fillna("Unknown")
    df["total"] = (df["quantity"] * df["unit_price"]) * (1 - df["discount_pct"] / 100)
    df["total"] = df["total"].round(2)

    logger.info(f"Transform finished, {before} rows in, {len(df)} rows out")
    return df


def validate(df):
    """Check the dataframe against the rules in great_expectations/sales_suite.json.
    Returns (passed: bool, failures: list of strings)."""
    logger.info("Validation started")

    with open(SUITE_PATH) as f:
        suite = json.load(f)

    thresholds = {
        "MIN_QUANTITY": MIN_QUANTITY,
        "MIN_UNIT_PRICE": MIN_UNIT_PRICE,
        "MAX_DISCOUNT_PCT": MAX_DISCOUNT_PCT,
    }

    def resolve(value):
        return thresholds[value] if isinstance(value, str) else value

    failures = []

    for rule in suite["expectations"]:
        rtype = rule["expectation_type"]
        kwargs = rule["kwargs"]
        col = kwargs["column"]

        if rtype == "expect_column_values_to_not_be_null":
            bad = df[col].isna().sum()
            if bad > 0:
                failures.append(f"{col}: {bad} null value(s)")

        elif rtype == "expect_column_values_to_be_unique":
            bad = df[col].duplicated().sum()
            if bad > 0:
                failures.append(f"{col}: {bad} duplicate value(s)")

        elif rtype == "expect_column_values_to_be_between":
            min_v = resolve(kwargs.get("min_value"))
            max_v = resolve(kwargs.get("max_value"))
            strict_min = kwargs.get("strict_min", False)
            series = df[col]

            if min_v is not None:
                bad = (series <= min_v).sum() if strict_min else (series < min_v).sum()
                if bad > 0:
                    failures.append(f"{col}: {bad} value(s) below allowed minimum ({min_v})")

            if max_v is not None:
                bad = (series > max_v).sum()
                if bad > 0:
                    failures.append(f"{col}: {bad} value(s) above allowed maximum ({max_v})")

    passed = len(failures) == 0
    if passed:
        logger.info("Validation passed, all 5 rules satisfied")
    else:
        for f_ in failures:
            logger.error(f"Validation failed - {f_}")

    return passed, failures


def load(df):
    """Load rows into SQLite. Clears the table first so re-running the
    pipeline never creates duplicate rows (idempotency)."""
    logger.info(f"Load started, writing to {DB_PATH}")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales_data (
            order_id INTEGER PRIMARY KEY,
            customer TEXT,
            quantity INTEGER,
            unit_price REAL,
            discount_pct REAL,
            order_date TEXT,
            total REAL
        )
    """)
    conn.execute("DELETE FROM sales_data")  # idempotent: clear before reload
    df.to_sql("sales_data", conn, if_exists="append", index=False)
    conn.commit()
    row_count = conn.execute("SELECT COUNT(*) FROM sales_data").fetchone()[0]
    conn.close()

    logger.info(f"Load finished, {row_count} rows now in sales_data")
    return row_count


def main():
    start = datetime.now()
    logger.info("=== Pipeline run started ===")

    try:
        raw_df = extract(RAW_DATA_PATH)
        clean_df = transform(raw_df)

        passed, failures = validate(clean_df)
        if not passed:
            logger.error(f"Pipeline halted, {len(failures)} validation rule(s) failed")
            print("Validation failed, pipeline stopped. See pipeline.log for details:")
            for f_ in failures:
                print(f" - {f_}")
            sys.exit(1)

        row_count = load(clean_df)
        print(f"Pipeline finished. {row_count} rows loaded into {DB_PATH}")

    except Exception as e:
        logger.exception(f"Pipeline failed with an error: {e}")
        print(f"Pipeline failed: {e}")
        sys.exit(1)

    finally:
        end = datetime.now()
        logger.info(f"=== Pipeline run finished, duration: {end - start} ===")


if __name__ == "__main__":
    main()
