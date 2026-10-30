
from pyspark.sql import SparkSession


def create_spark_session():
    """
    Creates and returns a Spark session for our
    E-Commerce Sales Analytics application.
    """

    # Initialize the Spark session
    spark = (
        SparkSession.builder
        # Set the name of our Spark application
        .appName("EcommerceSalesAnalytics")

        # Run Spark locally using all available CPU cores
        .master("local[*]")

        # Create a new session or reuse an existing one
        .getOrCreate()
    )

    # Reduce unnecessary Spark logs in the terminal
    spark.sparkContext.setLogLevel("ERROR")

    return spark
