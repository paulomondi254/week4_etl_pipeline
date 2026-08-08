# week4_etl_pipeline

Small ETL pipeline that reads daily sales data from a CSV, cleans it, checks it
against 5 data quality rules, and loads it into a SQLite database.

## What it does

1. **Extract** - reads the CSV listed in `.env`.
2. **Transform** - fills missing customer names and calculates a `total` column.
3. **Validate** - checks the data against the rules in `great_expectations/sales_suite.json`.
   If any rule fails, the pipeline stops and nothing is loaded.
4. **Load** - clears the `sales_data` table and reloads it. Running the script
   twice in a row will never create duplicate rows.

Every step writes to `pipeline.log` (start/end times, row counts, and any errors).

## Setup

1. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

2. Copy the env template and edit if needed:

   ```
   cp .env.example .env
   ```

   The defaults already point at the sample data in `data/raw/`.

## Running it

```
python run_pipeline.py
```

On success you'll see something like:

```
Pipeline finished. 9 rows loaded into data/processed/sales.db
```

There's also a deliberately messy file, `data/raw/sales.csv`, with a
duplicate order and a missing price. Point `RAW_DATA_PATH` in `.env` at it
to see the validation step stop the pipeline before anything gets loaded.

## Data quality rules

Defined in `great_expectations/sales_suite.json`:

1. `order_id` can't be null
2. `order_id` must be unique (no duplicate orders)
3. `quantity` must be at least `MIN_QUANTITY`
4. `unit_price` must be greater than `MIN_UNIT_PRICE`
5. `discount_pct` must be between 0 and `MAX_DISCOUNT_PCT`

Thresholds live in `.env` so they can change without touching code.

## Automation

See `automation/cron_setup.txt` for the cron entry (and Task Scheduler
equivalent) used to run this daily.

## Files

```
run_pipeline.py               main script
.env.example                  config template
great_expectations/           validation rules
data/raw/                     sample input CSVs
data/processed/               SQLite output (created on run)
automation/cron_setup.txt     scheduling proof
pipeline.log                  created on run
```
