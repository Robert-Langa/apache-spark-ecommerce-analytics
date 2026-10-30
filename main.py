
# Import the Spark session configuration
from src.spark_session import create_spark_session

# Import our data loading function
from src.load_data import load_sales_data

# Import our data cleaning function
from src.clean_data import clean_sales_data

# Import our analytics functions
from src.sales_analysis import (
    calculate_revenue,
    revenue_by_category
)


def main():
    """
    Runs the E-Commerce Sales Analytics pipeline.

    Pipeline:
    1. Create Spark session
    2. Load sales data
    3. Clean and validate data
    4. Calculate transaction revenue
    5. Analyze revenue by category
    """

    # Step 1: Initialize Apache Spark
    spark = create_spark_session()

    try:
        print("Starting E-Commerce Analytics...")

        # Step 2: Load the original CSV dataset
        df = load_sales_data(
            spark,
            "data/raw/sales.csv"
        )

        print("\nOriginal Sales Data:")
        df.show(truncate=False)

        # Step 3: Clean the dataset before analysis
        cleaned_df = clean_sales_data(df)

        print("\nCleaned Sales Data:")
        cleaned_df.show(truncate=False)

        # Display the schema after cleaning
        print("\nCleaned Dataset Schema:")
        cleaned_df.printSchema()

        # Step 4: Calculate revenue for each transaction
        sales_df = calculate_revenue(cleaned_df)

        print("\nSales With Revenue:")
        sales_df.show(truncate=False)

        # Step 5: Calculate total revenue by category
        category_df = revenue_by_category(sales_df)

        print("\nRevenue By Category:")
        category_df.show(truncate=False)

    finally:
        # Stop Spark and release resources
        spark.stop()


# Execute main only when running this file directly
if __name__ == "__main__":
    main()
