import logging
import os
from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import argparse
import boto3
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StockAnalyzer:
    """Class for stock data analysis calculations"""

    def __init__(self, df: DataFrame):
        self.df = df.cache()
        self.df_with_returns = None

    def calculate_daily_returns(self) -> DataFrame:
        """Calculate daily returns for each stock"""
        if self.df_with_returns is None:
            window_spec = Window.partitionBy("ticker").orderBy("Date")

            df_with_prev = self.df.withColumn(
                "prev_close",
                F.lag("close", 1).over(window_spec)
            )

            self.df_with_returns = df_with_prev.withColumn(
                "daily_return",
                F.when(F.col("prev_close").isNotNull(),
                       ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100)
                .otherwise(None)
            ).cache()

        return self.df_with_returns

    def calculate_average_daily_returns(self) -> DataFrame:
        """Compute the average daily return of all stocks for every date"""
        logger.info("Calculating average daily returns by date")

        df_with_returns = self.calculate_daily_returns()

        avg_returns = df_with_returns \
            .filter(F.col("daily_return").isNotNull()) \
            .groupBy("Date") \
            .agg(F.avg("daily_return").alias("average_return")) \
            .orderBy("Date")

        # Add year column for partitioning
        avg_returns_with_year = avg_returns.withColumn(
            "year",
            F.year(F.to_date("Date", "yyyy-MM-dd"))
        )

        return avg_returns_with_year

    def find_highest_worth_stock(self) -> DataFrame:
        """Find stock with highest average worth (close * volume)"""
        logger.info("Finding highest worth stock")

        df_with_worth = self.df.withColumn(
            "worth",
            F.col("close") * F.col("volume")
        )

        avg_worth = df_with_worth \
            .groupBy("ticker") \
            .agg(F.avg("worth").alias("value")) \
            .orderBy(F.desc("value")) \
            .limit(1)

        return avg_worth

    def find_most_volatile_stock(self) -> DataFrame:
        """Find most volatile stock by annualized standard deviation"""
        logger.info("Finding most volatile stock")

        df_with_returns = self.calculate_daily_returns()

        # 252 trading days per year
        trading_days_per_year = 252

        volatility = df_with_returns \
            .filter(F.col("daily_return").isNotNull()) \
            .groupBy("ticker") \
            .agg(
            (F.stddev("daily_return") * F.sqrt(F.lit(trading_days_per_year))).alias("standard_deviation") # Annualized volatility formula: stddev of daily returns \* sqrt(252)
        ) \
            .orderBy(F.desc("standard_deviation")) \
            .limit(1)

        return volatility

    def find_top_thirty_day_returns(self) -> DataFrame:
        """Find top three 30-day return dates by ticker"""
        logger.info("Finding top thirty-day returns")

        window_spec = Window.partitionBy("ticker").orderBy("Date")
        thirty_days_lookback = 30

        df_with_thirty_day_prev = self.df.withColumn(
            "close_thirty_days_ago",
            F.lag("close", thirty_days_lookback).over(window_spec)
        )

        df_with_thirty_day_return = df_with_thirty_day_prev.withColumn(
            "return_thirty_days",
            F.when(F.col("close_thirty_days_ago").isNotNull(),
                   ((F.col("close") - F.col("close_thirty_days_ago")) / F.col("close_thirty_days_ago")) * 100)
            .otherwise(None)
        )

        top_returns = df_with_thirty_day_return \
            .filter(F.col("return_thirty_days").isNotNull()) \
            .select("ticker", "Date", "return_thirty_days") \
            .orderBy(F.desc("return_thirty_days")) \
            .limit(3) \
            .select("ticker", F.col("Date").alias("date"))

        return top_returns

    def run_all_analyses(self) -> List[Dict[str, DataFrame]]:
        """Run all analyses and return results as list of dicts"""
        results = [
            {"avg_daily_returns": self.calculate_average_daily_returns()},
            {"highest_worth_stock": self.find_highest_worth_stock()},
            {"most_volatile_stock": self.find_most_volatile_stock()},
            {"top_thirty_day_returns": self.find_top_thirty_day_returns()}
        ]
        return results

class SparkUtility:
    """Utility class for Spark operations"""

    def __init__(self, app_name: str = "StockAnalysis"):
        self.app_name = app_name
        self.spark = None

    def create_spark_session(self) -> SparkSession:
        """Create optimized Spark session for AWS Glue environment"""
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .getOrCreate()

        logger.info(f"Spark session created: {self.spark.version}")
        return self.spark

    def read_csv_with_schema(self, file_path: str) -> DataFrame:
        """Read CSV file with predefined schema"""

        schema = StructType([
            StructField("Date", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("ticker", StringType(), True)
        ])

        df = self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(file_path)

        # logger.info(f"Loaded {df.count()} records from {file_path}")
        df.show(5)  # Display first 5 rows for verification

        return df

    def stop_spark(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


class AWSConnectorUtility:
    """Utility class for AWS S3 operations"""


    def __init__(self, output_bucket: str = None, input_bucket: str = None):
        self.output_bucket = output_bucket
        self.input_bucket = input_bucket

    @staticmethod
    def download_data_file_from_s3(file_path: str):
        """Download data file from S3 to local filesystem"""
        parsed_url = urlparse(file_path)
        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        s3 = boto3.client('s3')
        local_file_path = os.path.join(os.getcwd(), os.path.basename(key))

        logger.info(f"Downloading {key} from bucket {bucket_name} to {local_file_path}")
        s3.download_file(bucket_name, key, local_file_path)
        logger.info(f"Downloaded file to: {local_file_path}")

        return local_file_path

    def write_dataframe_to_s3(self, df: DataFrame, analysis_name: str,
                              partition_cols: Optional[List[str]] = None):
        """Write DataFrame to S3 in Parquet format"""
        output_path = f"s3://{self.output_bucket}/results/{analysis_name}/"
        logger.info(f"Writing {analysis_name} to: {output_path}")

        writer = df.coalesce(1).write.mode("overwrite")

        if partition_cols:
            writer = writer.partitionBy(partition_cols)

        writer.parquet(output_path)
        logger.info(f"Successfully wrote {analysis_name} to S3")


    def write_results_list(self, results: List[Dict[str, DataFrame]]):
        """Write list of result dictionaries to S3"""
        # Define partition columns for specific analyses
        partition_config = {
            "avg_daily_returns": ["year"],
            "highest_worth_stock": None,
            "most_volatile_stock": None,
            "top_thirty_day_returns": None
        }

        for result_dict in results:
            for analysis_name, df in result_dict.items():
                partition_cols = partition_config.get(analysis_name)
                self.write_dataframe_to_s3(df, analysis_name, partition_cols)


def display_results(results: List[Dict[str, DataFrame]], local_mode: bool):
    """Display results in local mode"""
    if local_mode:
        logger.info("\n=== SAMPLE RESULTS ===")

        display_config = {
            "avg_daily_returns": (10, False, "Average Daily Returns (first ten rows)"),
            "highest_worth_stock": (20, False, "Highest Worth Stock"),
            "most_volatile_stock": (20, False, "Most Volatile Stock"),
            "top_thirty_day_returns": (20, False, "Top Thirty-Day Returns")
        }

        for result_dict in results:
            for analysis_name, df in result_dict.items():
                rows, truncate, title = display_config.get(
                    analysis_name, (20, False, analysis_name)
                )
                logger.info(f"\n{title}:")
                df.show(rows, truncate=truncate)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Stock Data Analysis')
    parser.add_argument('--input-path', required=True, help='S3 path to input CSV file')
    parser.add_argument('--output-bucket', required=True, help='S3 bucket for output files')
    parser.add_argument('--local-mode', action='store_true', help='Run in local mode for testing')
    return parser.parse_args()

def main():
    """Main execution function"""
    # Step One: Read arguments
    args = parse_arguments()

    # Step Two: Instantiate Spark session and read DataFrame
    spark_util = SparkUtility()
    spark_util.create_spark_session()

    try:
        # For local mode, adjust the input path
        if not args.local_mode:
            logger.info("Running in production mode, using S3 input path")
            AWSConnectorUtility.download_data_file_from_s3(args.input_path)
            # df = spark_util.read_csv_with_schema(input_path)
        else:
            logger.info("Running in local mode, adjusting input path")
            input_path = f"../{args.input_path}"
            df = spark_util.read_csv_with_schema(input_path)

        # Step Three: Instantiate StockAnalyzer
        stock_analyzer = StockAnalyzer(df)

        # Step Four: Perform analyses and collect results
        results = stock_analyzer.run_all_analyses()

        # Display results in local mode
        display_results(results, args.local_mode)

        # Step Five: Write results to S3 (skip in local mode)
        if not args.local_mode:
            s3_connector = AWSConnectorUtility(args.output_bucket)
            s3_connector.write_results_list(results)
            logger.info("All results written to S3 successfully!")
        else:
            logger.info("Local mode: Skipping S3 writes")

    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        raise
    finally:
        spark_util.stop_spark()


if __name__ == "__main__":
    main()