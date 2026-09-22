"""Environment check only. This is NOT the CardDemo migration implementation."""
import json
import os
import platform
import sys
from decimal import Decimal
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession, functions as F, types as T

spark = (SparkSession.builder.master("local[2]").appName("migration-prep-smoke")
         .config("spark.ui.enabled", "false")
         .config("spark.driver.bindAddress", "127.0.0.1")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
try:
    spark.sparkContext.setLogLevel("ERROR")
    schema = T.StructType([T.StructField("value", T.DecimalType(12, 2), False)])
    frame = spark.createDataFrame([(Decimal("10.01"),), (Decimal("-2.02"),)], schema)
    total = frame.select(F.sum("value").alias("total")).first()["total"]
    assert total == Decimal("7.99"), total
    result = {"status": "passed", "python": platform.python_version(),
              "spark": spark.version, "java": spark._jvm.java.lang.System.getProperty("java.version"),
              "decimal_sum": str(total), "scope": "local Spark runtime only; no migration or Databricks execution"}
    print(json.dumps(result, indent=2))
    destination = Path(__file__).resolve().parents[1] / "artifacts/spark-smoke.json"
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(json.dumps(result, indent=2) + "\n")
finally:
    spark.stop()
