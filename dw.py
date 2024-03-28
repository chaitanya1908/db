from pyspark.sql import SparkSession

import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *


# if len(sys.argv) != 4:
#     print("Usage: spark-submit dw.py <customer_parquet> <city_parquet> <sales_parquet>", file=sys.stderr)
#     sys.exit(-1)

# Create SparkSession
spark = SparkSession.builder.appName("DW House").getOrCreate()

# Read file paths from command-line arguments
customer_parquet = sys.argv[1]
city_parquet = sys.argv[2]
sales_parquet = sys.argv[3]

# Load data from Parquet files
customer_df = spark.read.parquet(customer_parquet)
city_df = spark.read.parquet(city_parquet)
sales_df = spark.read.parquet(sales_parquet)

print("Number of rows in customer_df:", customer_df.count())
print("Number of rows in city_df:", city_df.count())
print("Number of rows in sales_df:", sales_df.count())

customer_df.show(5)
city_df.show(5)
sales_df.show(5)
   


# Create temporary views for SQL querying
customer_df.createOrReplaceTempView("customer")
city_df.createOrReplaceTempView("city")
sales_df.createOrReplaceTempView("sales")

# Creating Date Table from Sales Table
date_df = spark.sql("""
    SELECT DISTINCT
        InvoiceDate AS Date,
        WEEKOFYEAR(InvoiceDate) AS WeekNumber,
        DAYOFWEEK(InvoiceDate) AS DayOfWeek,
        MONTH(InvoiceDate) AS Month,
        QUARTER(InvoiceDate) AS Quarter,
        YEAR(InvoiceDate) AS Year
    FROM sales
""")

date_df.createOrReplaceTempView("date")
date_df.show(5)

spark.stop()