import shutil
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, broadcast, when, sum, count, max, avg, first, last, mode
from pyspark.sql.types import StringType, FloatType, DateType
import json
import os

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: Ingest Bronze] Starting ingestion")
        
        partition_path = os.path.join(output_path, "transactions", "ingestion_timestamp={}".format(run_date))
        shutil.rmtree(partition_path, ignore_errors=True)
        
        transactions_df = (spark.read.format("csv")
                          .option("header", "true")
                          .option("inferSchema", "false")
                          .load(input_path)
                          .withColumnRenamed("_c0", "transaction_id")
                          .withColumnRenamed("_c1", "merchant_id")
                          .withColumnRenamed("_c2", "amount")
                          .withColumnRenamed("_c3", "transaction_date")
                          .withColumnRenamed("_c4", "status")
                          .withColumn("ingestion_timestamp", lit(run_date))
                           .withColumn("source_file", lit("transactions.csv"))
                          .withColumn("pipeline_run_id", lit(run_id)))
        
        logging.info("[Stage: Ingest Bronze] Transactions input count: {:,}".format(transactions_df.count()))
        
        merchants_df = (spark.read.format("csv")
                       .option("header", "true")
                        .option("inferSchema", "false")
                        .load(input_path.replace("transactions.csv", "merchants.csv"))
                        .withColumnRenamed("_c0", "merchant_id")
                        .withColumnRenamed("_c1", "merchant_name")
                       .withColumnRenamed("_c2", "category")
                       .withColumnRenamed("_c3", "city"))
        
        logging.info("[Stage: Ingest Bronze] Merchants input count: {:,}".format(merchants_df.count()))
        
        partition_path = os.path.join(output_path, "merchants", "ingestion_timestamp={}".format(run_date))
        shutil.rmtree(partition_path, ignore_errors=True)
        
        transactions_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(os.path.join(output_path, "transactions"))
        merchants_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(os.path.join(output_path, "merchants"))
        
        logging.info("[Stage: Ingest Bronze] Completed ingestion")
    except Exception as e:
        logging.error("[Stage: Ingest Bronze] Error: {}".format(str(e)))
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: Transform Silver] Starting transformation")
        
        transactions_df = (spark.read.format("parquet")
                           .option("basePath", bronze_path)
                          .load(os.path.join(bronze_path, "ingestion_timestamp={}".format(run_date)))
                           .withColumn("transaction_date", col("transaction_date").cast(DateType()))
                          .withColumn("amount", col("amount").cast(FloatType())))
        
        logging.info("[Stage: Transform Silver] Transactions input count: {:,}".format(transactions_df.count()))
        
        merchants_df = (spark.read.format("parquet")
                        .option("basePath", merchants_path)
                        .load(os.path.join(merchants_path, "ingestion_timestamp={}".format(run_date)))
                        .cache())
        
        transactions_df = transactions_df.withColumn("transaction_id", col("transaction_id").cast(StringType())) \
                                          .withColumn("merchant_id", col("merchant_id").cast(StringType()))
        
        transactions_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
        
        logging.info("[Stage: Transform Silver] Transactions after filter count: {:,}".format(transactions_df.count()))
        
        transactions_dedup_df = (transactions_df.groupBy("transaction_id")
                                .agg({"ingestion_timestamp": "max"})
                                .withColumnRenamed("max(ingestion_timestamp)", "ingestion_timestamp"))
        transactions_df = transactions_df.join(transactions_dedup_df, ["transaction_id", "ingestion_timestamp"], "inner")
        
        logging.info("[Stage: Transform Silver] Transactions after dedup count: {:,}".format(transactions_df.count()))
        
        transactions_enriched_df = (transactions_df.join(broadcast(merchants_df), transactions_df.merchant_id == merchants_df.merchant_id, "left_outer")
                                    .withColumn("quality_flag", when(col("merchant_id").isNull(), "UNMATCHED").otherwise("CLEAN")))
        
        partition_path = os.path.join(output_path, "transaction_date={}".format(run_date))
        shutil.rmtree(partition_path, ignore_errors=True)
        
        transactions_enriched_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)
        
        logging.info("[Stage: Transform Silver] Completed transformation")
    except Exception as e:
        logging.error("[Stage: Transform Silver] Error: {}".format(str(e)))
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Merchant Performance] Starting aggregation")
        
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)  # Partition pruning
        
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        merchant_performance_df = completed_txns.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date") \
           .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("txn_count")
            )
        
        all_txns = silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date") \
           .agg(
                count(when(col("status") == "FAILED", 1)).alias("failed_txns"),
                count("*").alias("total_txns")
            )
        failure_rate_df = all_txns.withColumn("failure_rate_pct", (col("failed_txns") / col("total_txns") * 100).cast(FloatType()))
        
        merchant_performance_df = merchant_performance_df.join(failure_rate_df["merchant_id", "transaction_date", "failure_rate_pct"], 
                                                              ["merchant_id", "transaction_date"], 
                                                              "left")
        
        partition_path = os.path.join(output_path, "transaction_date={}".format(run_date))
        shutil.rmtree(partition_path, ignore_errors=True)
        
        merchant_performance_df.write.partitionBy("transaction_date").mode("overwrite").parquet(output_path)
        
        logging.info("[Stage: Build Merchant Performance] Completed aggregation")
    except Exception as e:
        logging.error("[Stage: Build Merchant Performance] Error: {}".format(str(e)))
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("[Stage: Build Customer LTV] Starting aggregation")
        
        silver_df = spark.read.parquet(silver_path)
        
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        customer_ltv_df = completed_txns.groupBy("customer_id") \
          .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                first("transaction_date").alias("first_txn_date"),
                last("transaction_date").alias("last_txn_date"),
                mode("payment_method").alias("preferred_payment_method")
            )
        
        customer_ltv_df.write.mode("overwrite").parquet(output_path)
        
        logging.info("[Stage: Build Customer LTV] Completed aggregation")
    except Exception as e:
        logging.error("[Stage: Build Customer LTV] Error: {}".format(str(e)))
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Daily Summary] Starting aggregation")
        
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)  # Partition pruning
        
        daily_summary_df = silver_df.groupBy("transaction_date") \
           .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
                count("*").alias("total_txns"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("merchant_id").alias("unique_merchants")
            )
        
        all_txns = silver_df.groupBy("transaction_date") \
           .agg(
                count(when(col("status") == "FAILED", 1)).alias("failed_txns"),
                count("*").alias("total_txns")
            )
        failure_rate_df = all_txns.withColumn("failure_rate_pct", (col("failed_txns") / col("total_txns") * 100).cast(FloatType()))
        
        daily_summary_df = daily_summary_df.join(failure_rate_df["transaction_date", "failure_rate_pct"], "transaction_date", "left")
        
        partition_path = os.path.join(output_path, "transaction_date={}".format(run_date))
        shutil.rmtree(partition_path, ignore_errors=True)
        
        daily_summary_df.write.partitionBy("transaction_date").mode("overwrite").parquet(output_path)
        
        logging.info("[Stage: Build Daily Summary] Completed aggregation")
    except Exception as e:
        logging.error("[Stage: Build Daily Summary] Error: {}".format(str(e)))
        raise

def run_pipeline(spark, input_path, bronze_path, merchants_path, silver_path, gold_output_dir, run_date, run_id):
    try:
        started_at = datetime.now().isoformat()
        
        ingest_bronze(spark, input_path, bronze_path, run_date, run_id)
        transform_silver(spark, bronze_path, merchants_path, silver_path, run_date)
        
        merchant_performance_output = f"{gold_output_dir}/merchant_performance"
        customer_ltv_output = f"{gold_output_dir}/customer_ltv"
        daily_summary_output = f"{gold_output_dir}/daily_summary"
        
        build_merchant_performance(spark, silver_path, merchant_performance_output, run_date)
        build_customer_ltv(spark, silver_path, customer_ltv_output)
        build_daily_summary(spark, silver_path, daily_summary_output, run_date)
        
        completed_at = datetime.now().isoformat()
        
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": run_id,
            "run_status": "SUCCESS",
            "started_at": started_at,
            "completed_at": completed_at,
            "error_message": None,
            "row_counts": {
                "ingest_bronze_transactions_input_count": None,
                "ingest_bronze_merchants_input_count": None,
                "transform_silver_transactions_input_count": None,
                "transform_silver_transactions_after_filter_count": None,
                "transform_silver_transactions_after_dedup_count": None,
                "build_merchant_performance_output_count": None,
                "build_customer_ltv_output_count": None,
                "build_daily_summary_output_count": None
            }
        }
        
        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)
    except Exception as e:
        completed_at = datetime.now().isoformat()
        
        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": run_id,
            "run_status": "FAILED",
            "started_at": started_at,
            "completed_at": completed_at,
            "error_message": str(e),
            "row_counts": {
                "ingest_bronze_transactions_input_count": None,
                "ingest_bronze_merchants_input_count": None,
                "transform_silver_transactions_input_count": None,
                "transform_silver_transactions_after_filter_count": None,
                "transform_silver_transactions_after_dedup_count": None,
                "build_merchant_performance_output_count": None,
                "build_customer_ltv_output_count": None,
                "build_daily_summary_output_count": None
            }
        }
        
        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f)
        
        raise

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Sigma DataTech Transaction Analytics Pipeline").getOrCreate()
    
    input_path = "s3://your-bucket/input/"
    bronze_path = "s3://your-bucket/bronze/"
    merchants_path = "s3://your-bucket/merchants/"
    silver_path = "s3://your-bucket/silver/"
    gold_output_dir = "s3://your-bucket/gold/"
    run_date = "2026-05-27"
    run_id = "run_id_12345"
    
    run_pipeline(spark, input_path, bronze_path, merchants_path, silver_path, gold_output_dir, run_date, run_id)
