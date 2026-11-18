"""
ingest_profile.py
Step 1 of the NYC Pet Services Demand Analysis pipeline.

Reads raw NYC 311 and NYC Dog Licensing CSVs, normalizes column names,
profiles data quality (row counts, nulls, ZIP validity, duplicates),
filters 311 to animal-related complaints, and writes cleaned Parquet
plus a JSON profile report.

Usage (local):
    spark-submit ingest_profile.py \
        --311 data/raw/311_service_requests.csv \
        --dogs data/raw/dog_licensing.csv \
        --out data/processed \
        --report reports/profile_report.json

Usage (Dataproc / HDFS): pass hdfs:// or gs:// paths for --311, --dogs, --out.
The report is always written to the local filesystem.
"""

import argparse
import json
import os
import re
import time

from pyspark.sql import DataFrame, SparkSession, functions as F

# Rough bounds for NYC ZIP codes (10001-11697). Used only as a sanity check.
NYC_ZIP_MIN = 10001
NYC_ZIP_MAX = 11697

# Complaint types are NOT hardcoded: the script lists every type matching this
# pattern so you can review them and pick the final set for the analysis.
ANIMAL_PATTERN = r"(?i)animal|\bdog|bark|\bpet\b"


def normalize_columns(df: DataFrame) -> DataFrame:
    """Lowercase column names and replace non-alphanumerics with underscores."""
    for col in df.columns:
        new = re.sub(r"[^0-9a-zA-Z]+", "_", col.strip()).strip("_").lower()
        if new != col:
            df = df.withColumnRenamed(col, new)
    return df


def find_column(df: DataFrame, candidates, contains=None):
    """Return the first column matching a candidate name, else the first
    column whose name contains `contains`, else None."""
    for c in candidates:
        if c in df.columns:
            return c
    if contains:
        for c in df.columns:
            if contains in c:
                return c
    return None


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("multiLine", False)
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(path)
    )
    return normalize_columns(df)


def clean_zip(df: DataFrame, zip_col: str) -> DataFrame:
    """Add `zip5` (5-digit string or null) and `zip_valid` (within NYC range)."""
    digits = F.regexp_extract(F.trim(F.col(zip_col).cast("string")), r"^(\d{5})", 1)
    df = df.withColumn("zip5", F.when(digits == "", None).otherwise(digits))
    return df.withColumn(
        "zip_valid",
        F.col("zip5").cast("int").between(NYC_ZIP_MIN, NYC_ZIP_MAX),
    )


def null_counts(df: DataFrame) -> dict:
    row = df.select(
        [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    ).first()
    return {c: int(row[c] or 0) for c in df.columns}


def profile_basic(df: DataFrame, name: str) -> dict:
    print(f"\n=== {name}: schema ===")
    df.printSchema()
    total = df.count()
    print(f"{name}: {total:,} rows, {len(df.columns)} columns")
    return {
        "rows": total,
        "columns": df.columns,
        "null_counts": null_counts(df),
    }


def profile_zip(df: DataFrame, total: int) -> dict:
    missing = df.filter(F.col("zip5").isNull()).count()
    invalid = df.filter(F.col("zip5").isNotNull() & ~F.col("zip_valid")).count()
    distinct_valid = (
        df.filter(F.col("zip_valid")).select("zip5").distinct().count()
    )
    return {
        "missing_zip": missing,
        "missing_zip_pct": round(100 * missing / total, 2) if total else None,
        "outside_nyc_range": invalid,
        "distinct_valid_zips": distinct_valid,
    }


def process_311(spark, path, out_dir) -> dict:
    t0 = time.time()
    df = read_csv(spark, path)
    report = profile_basic(df, "311")

    zip_col = find_column(df, ["incident_zip"], contains="zip")
    # NYC renamed this column; the export may use either name.
    type_col = find_column(
        df, ["complaint_type", "problem_formerly_complaint_type"], contains="complaint_type"
    )
    detail_col = find_column(
        df, ["descriptor", "problem_detail_formerly_descriptor"], contains="descriptor"
    )
    date_col = find_column(df, ["created_date"])
    key_col = find_column(df, ["unique_key"])
    if not (zip_col and type_col):
        raise ValueError(f"Could not find ZIP/complaint columns in: {df.columns}")
    report["detected_columns"] = {
        "zip": zip_col, "complaint_type": type_col,
        "created_date": date_col, "unique_key": key_col,
    }

    df = clean_zip(df, zip_col)
    report["zip_quality"] = profile_zip(df, report["rows"])

    if key_col:
        distinct_keys = df.select(key_col).distinct().count()
        report["duplicate_unique_keys"] = report["rows"] - distinct_keys

    # Review step: which complaint types look animal-related?
    # Match on the complaint type OR its detail, since barking and waste
    # complaints may sit under a non-animal type (e.g. a noise category).
    animal_match = F.col(type_col).rlike(ANIMAL_PATTERN)
    if detail_col:
        animal_match = animal_match | F.col(detail_col).rlike(ANIMAL_PATTERN)
    animal = df.filter(animal_match)
    types = (
        animal.groupBy(type_col).count().orderBy(F.desc("count")).collect()
    )
    report["animal_complaint_types"] = {r[type_col]: r["count"] for r in types}
    print("\n=== 311 complaint types matching animal pattern ===")
    for r in types:
        print(f"  {r[type_col]}: {r['count']:,}")

    report["animal_rows"] = sum(r["count"] for r in types)

    # Type + detail pairs help separate e.g. barking from waste complaints.
    if detail_col:
        pairs = (
            animal.groupBy(type_col, detail_col).count().orderBy(F.desc("count")).limit(40).collect()
        )
        report["animal_type_detail_top40"] = [
            {"type": r[type_col], "detail": r[detail_col], "count": r["count"]} for r in pairs
        ]
        print("\n=== Top complaint type + detail pairs (animal pattern) ===")
        for r in pairs:
            print(f"  {r[type_col]} | {r[detail_col]}: {r['count']:,}")

    if date_col:
        # Parse year loosely; 311 dates are usually 'MM/dd/yyyy hh:mm:ss a'.
        year = F.regexp_extract(F.col(date_col), r"(\d{4})", 1)
        by_year = (
            animal.withColumn("year", year).groupBy("year").count().orderBy("year").collect()
        )
        report["animal_rows_by_year"] = {r["year"]: r["count"] for r in by_year}

    (
        animal.filter(F.col("zip_valid"))
        .write.mode("overwrite")
        .parquet(os.path.join(out_dir, "311_animal_clean"))
    )
    report["seconds"] = round(time.time() - t0, 1)
    return report


def process_dogs(spark, path, out_dir) -> dict:
    t0 = time.time()
    df = read_csv(spark, path)
    report = profile_basic(df, "dog_licensing")

    zip_col = find_column(df, ["zipcode", "zip_code"], contains="zip")
    if not zip_col:
        raise ValueError(f"Could not find ZIP column in: {df.columns}")
    report["detected_columns"] = {"zip": zip_col}

    df = clean_zip(df, zip_col)
    report["zip_quality"] = profile_zip(df, report["rows"])

    # Exact duplicate rows.
    distinct_rows = df.dropDuplicates(df.columns).count()
    report["exact_duplicate_rows"] = report["rows"] - distinct_rows

    # Heuristic for double counting (e.g. renewals across years): treat the same
    # name + gender + birth year + breed + ZIP as one dog. This is an
    # approximation, so compare both counts before choosing one.
    dog_cols = [
        c for c in ["animalname", "animalgender", "animalbirthyear", "breedname", "zip5"]
        if c in df.columns
    ]
    if len(dog_cols) >= 3:
        distinct_dogs = df.dropDuplicates(dog_cols).count()
        report["dedupe_key"] = dog_cols
        report["rows_after_heuristic_dedupe"] = distinct_dogs
        report["heuristic_duplicates"] = report["rows"] - distinct_dogs

    clean = df.filter(F.col("zip_valid")).dropDuplicates(dog_cols if dog_cols else df.columns)
    per_zip = clean.groupBy("zip5").agg(F.count("*").alias("licensed_dogs"))
    report["zips_with_dogs"] = per_zip.count()
    print("\n=== Top 10 ZIPs by licensed dogs (heuristic dedupe) ===")
    per_zip.orderBy(F.desc("licensed_dogs")).show(10, truncate=False)

    clean.write.mode("overwrite").parquet(os.path.join(out_dir, "dog_licensing_clean"))
    per_zip.write.mode("overwrite").parquet(os.path.join(out_dir, "dogs_per_zip"))
    report["seconds"] = round(time.time() - t0, 1)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--311", dest="path_311", required=True, help="311 CSV path")
    parser.add_argument("--dogs", dest="path_dogs", required=True, help="Dog licensing CSV path")
    parser.add_argument("--out", required=True, help="Output directory for cleaned Parquet")
    parser.add_argument("--report", default="reports/profile_report.json", help="Local JSON report path")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("nyc-pet-ingest-profile")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    report = {
        "311": process_311(spark, args.path_311, args.out),
        "dog_licensing": process_dogs(spark, args.path_dogs, args.out),
    }

    os.makedirs(os.path.dirname(args.report) or ".", exist_ok=True)
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nProfile report written to {args.report}")

    spark.stop()


if __name__ == "__main__":
    main()