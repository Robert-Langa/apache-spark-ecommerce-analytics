
from pyspark.sql import DataFrame, SparkSession


def load_sales_data(
    spark: SparkSession,
    file_path: str
) -> DataFrame:
    """
    Loads sales data from a CSV file into a
    Spark DataFrame.

    Args:
        spark: Active Spark session.
        file_path: Path to the CSV dataset.

    Returns:
        A Spark DataFrame containing the sales data.
    """

    # Read the CSV file using Spark
    df = (
        spark.read

        # Use the first row as column names
        .option("header", True)

        # Automatically detect column data types
        .option("inferSchema", True)

        # Load the dataset from the specified path
        .csv(file_path)
    )

    return df
