
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, to_date


def clean_sales_data(df: DataFrame) -> DataFrame:
    """
    Cleans and validates the e-commerce sales dataset.

    The cleaning process includes:
    1. Removing duplicate transactions.
    2. Removing records with missing required values.
    3. Removing transactions with invalid quantities or prices.
    4. Removing unnecessary whitespace.
    5. Converting order dates into the correct format.

    Args:
        df: The original Spark DataFrame.

    Returns:
        A cleaned Spark DataFrame.
    """

    # Step 1: Remove duplicate records based on order_id
    # Each order_id should be unique in our sample dataset
    df = df.dropDuplicates(["order_id"])

    # Step 2: Remove rows with missing required information
    df = df.dropna(
        subset=[
            "order_id",
            "customer_id",
            "product",
            "category",
            "quantity",
            "unit_price",
            "order_date"
        ]
    )

    # Step 3: Remove transactions with invalid values
    # Quantity and unit price must be greater than zero
    df = df.filter(
        (col("quantity") > 0) &
        (col("unit_price") > 0)
    )

    # Step 4: Remove leading and trailing whitespace
    # from text columns
    df = (
        df
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("product", trim(col("product")))
        .withColumn("category", trim(col("category")))
    )

    # Step 5: Convert order_date into Spark's DateType
    # Our CSV dataset uses the yyyy-MM-dd format
    df = df.withColumn(
        "order_date",
        to_date(col("order_date"), "yyyy-MM-dd")
    )

    # Step 6: Remove rows containing invalid dates
    # or empty text fields after trimming
    df = df.dropna(subset=["order_date"])

    df = df.filter(
        (col("customer_id") != "") &
        (col("product") != "") &
        (col("category") != "")
    )

    return df
