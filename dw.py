from pyspark.sql import SparkSession


from pyspark.sql.types import *
from pyspark.sql.functions import *


# Create SparkSession
spark = SparkSession.builder \
    .appName("DW House") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Load data from Parquet files
customer_df = spark.read.parquet("/home/vagrant/documents/db/DimCustomer.parquet")
city_df = spark.read.parquet("/home/vagrant/documents/db/dimension_city.parquet")
sales_df = spark.read.parquet("/home/vagrant/documents/db/fact_sale.parquet")

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

spark.stop()