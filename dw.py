from pyspark.sql import SparkSession

import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *



if __name__ == "__main__":
 if len(sys.argv) != 2:
    print("Usage: Flightscsv <file>", file=sys.stderr)
    sys.exit(-1)
    # Create SparkSession

spark = SparkSession.builder.appName("DW House").getOrCreate()

   
data_source = sys.argv[1]
fl_df = spark.read.format("parquet").option("header", "true").load(data_source)

# # Load data from Parquet files
# customer_df = spark.read.parquet("/home/vagrant/Documents/db/DimCustomer.parquet")
# city_df = spark.read.parquet("/home/vagrant/Documents/db/dimension_city.parquet")
# sales_df = spark.read.parquet("/home/vagrant/Documents/db/fact_sale.parquet")

customer_df = spark.read.parquet('DimCustomer.parquet')
city_df = spark.read.parquet('dimension_city.parquet')
sales_df = spark.read.parquet('fact_sale.parquet')
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