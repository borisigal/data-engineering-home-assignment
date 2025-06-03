"""
Stock Data Analysis with PySpark
Compatible with Python 3.9 and PySpark 3.3.0
Optimized for large-scale data processing
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "StockAnalysis") -> SparkSession:
    """
    Create optimized Spark session for AWS Glue environment
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .getOrCreate()

    logger.info(f"Spark session created: {spark.version}")
    return spark


def calculate_daily_returns(df):
    """
    Calculate daily returns for each stock
    Returns = (current_close - previous_close) / previous_close * 100
    """
    # Window specification for each ticker ordered by date
    window_spec = Window.partitionBy("ticker").orderBy("Date")

    # Calculate previous day's close price
    df_with_prev = df.withColumn(
        "prev_close",
        F.lag("close", 1).over(window_spec)
    )

    # Calculate daily return percentage
    df_with_returns = df_with_prev.withColumn(
        "daily_return",
        F.when(F.col("prev_close").isNotNull(),
               ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100)
        .otherwise(None)
    )

    return df_with_returns


def objective_1_avg_daily_returns(df_with_returns):
    """
    Objective 1: Compute the average daily return of all stocks for every date
    """
    logger.info("Calculating Objective 1: Average daily returns by date")

    # Group by date and calculate average return
    avg_returns = df_with_returns \
        .filter(F.col("daily_return").isNotNull()) \
        .groupBy("Date") \
        .agg(F.avg("daily_return").alias("average_return")) \
        .orderBy("Date")

    return avg_returns


def objective_2_highest_worth_stock(df):
    """
    Objective 2: Stock with highest average worth (close * volume)
    """
    logger.info("Calculating Objective 2: Highest worth stock")

    # Calculate worth for each record
    df_with_worth = df.withColumn(
        "worth",
        F.col("close") * F.col("volume")
    )

    # Group by ticker and calculate average worth
    avg_worth = df_with_worth \
        .groupBy("ticker") \
        .agg(F.avg("worth").alias("value")) \
        .orderBy(F.desc("value")) \
        .limit(1)

    return avg_worth


def objective_3_most_volatile_stock(df_with_returns):
    """
    Objective 3: Most volatile stock by annualized standard deviation
    Annualized std = daily_std * sqrt(252)
    """
    logger.info("Calculating Objective 3: Most volatile stock")

    # Calculate standard deviation of daily returns by ticker
    # Multiply by sqrt(252) to annualize (252 trading days per year)
    volatility = df_with_returns \
        .filter(F.col("daily_return").isNotNull()) \
        .groupBy("ticker") \
        .agg(
            (F.stddev("daily_return") * F.sqrt(F.lit(252))).alias("standard_deviation")
        ) \
        .orderBy(F.desc("standard_deviation")) \
        .limit(1)

    return volatility


def objective_4_top_30day_returns(df):
    """
    Objective 4: Top three 30-day return dates by ticker
    """
    logger.info("Calculating Objective 4: Top 30-day returns")

    # Window specification for each ticker
    window_spec = Window.partitionBy("ticker").orderBy("Date")

    # Get close price from 30 days ago
    # Using lag(30) to look back exactly 30 rows
    df_with_30d_prev = df.withColumn(
        "close_30d_ago",
        F.lag("close", 30).over(window_spec)
    )

    # Calculate 30-day return
    df_with_30d_return = df_with_30d_prev.withColumn(
        "return_30d",
        F.when(F.col("close_30d_ago").isNotNull(),
               ((F.col("close") - F.col("close_30d_ago")) / F.col("close_30d_ago")) * 100)
        .otherwise(None)
    )

    # Get top 3 returns across all tickers and dates
    top_returns = df_with_30d_return \
        .filter(F.col("return_30d").isNotNull()) \
        .select("ticker", "Date", "return_30d") \
        .orderBy(F.desc("return_30d")) \
        .limit(3) \
        .select("ticker", F.col("Date").alias("date"))

    return top_returns


def write_results_to_s3(df, output_path: str, partition_cols=None):
    """
    Write DataFrame to S3 in Parquet format with optional partitioning
    """
    logger.info(f"Writing results to: {output_path}")

    writer = df.coalesce(1).write.mode("overwrite")

    if partition_cols:
        writer = writer.partitionBy(partition_cols)

    writer.parquet(output_path)
    logger.info("Write completed successfully")


def main():
    """
    Main execution function
    """
    # print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
    # print(f"PySpark version: {pyspark.sql.__version__}")


    parser = argparse.ArgumentParser(description='Stock Data Analysis')
    parser.add_argument('--input-path', required=True, help='S3 path to input CSV file')
    parser.add_argument('--output-bucket', required=True, help='S3 bucket for output files')
    parser.add_argument('--local-mode', action='store_true', help='Run in local mode for testing')

    args = parser.parse_args()

    # Create Spark session
    spark = create_spark_session()

    try:
        logger.info(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
        # Read input data
        logger.info(f"Reading data from: {args.input_path}")

        # Define schema for better performance with large files
        schema = StructType([
            StructField("Date", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("ticker", StringType(), True)
        ])

        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(f"../{args.input_path}")

        logger.info(f"Count: {df.count()}")
        print("Schema:")
        df.printSchema()
        print("First 5 rows:")
        df.show(5)


        # Cache the DataFrame for multiple operations
        df.cache()
        logger.info(f"Loaded {df.count()} records")

        # Calculate daily returns (needed for objectives 1 and 3)
        df_with_returns = calculate_daily_returns(df)
        df_with_returns.cache()

        # Objective 1: Average daily returns
        result1 = objective_1_avg_daily_returns(df_with_returns)

        # Add year column for partitioning
        # result1_partitioned = result1.withColumn("year", F.year(F.to_date("Date", "yyyy-MM-dd")))

        # output_path1 = f"s3://{args.output_bucket}/results/avg_daily_returns/"
        # write_results_to_s3(result1_partitioned, output_path1, ["year"])

        # Objective 2: Highest worth stock
        result2 = objective_2_highest_worth_stock(df)
        # output_path2 = f"s3://{args.output_bucket}/results/highest_worth_stock/"
        # write_results_to_s3(result2, output_path2)

        # Objective 3: Most volatile stock
        result3 = objective_3_most_volatile_stock(df_with_returns)
        # output_path3 = f"s3://{args.output_bucket}/results/most_volatile_stock/"
        # write_results_to_s3(result3, output_path3)

        # Objective 4: Top 30-day returns
        result4 = objective_4_top_30day_returns(df)
        # output_path4 = f"s3://{args.output_bucket}/results/top_30day_returns/"
        # write_results_to_s3(result4, output_path4)

        logger.info("All calculations completed successfully!")

        # Show sample results in local mode
        if args.local_mode:
            logger.info("\n=== SAMPLE RESULTS ===")
            logger.info("Objective 1 - Average Daily Returns (first 10 rows):")
            result1.show(10, truncate=False)

            logger.info("Objective 2 - Highest Worth Stock:")
            result2.show(truncate=False)

            logger.info("Objective 3 - Most Volatile Stock:")
            result3.show(truncate=False)

            logger.info("Objective 4 - Top 30-day Returns:")
            result4.show(truncate=False)


    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()