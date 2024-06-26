from pyspark.sql import SparkSession

import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName("DW House").getOrCreate()

# Read file paths from command-line arguments
customer_parquet = sys.argv[1]
city_parquet = sys.argv[2]
sales_parquet = sys.argv[3]
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
customer_df = spark.read.parquet(customer_parquet)
city_df = spark.read.parquet(city_parquet)
sales_df = spark.read.parquet(sales_parquet)
# Load data from Parquet files

customer_df.printSchema()
city_df.printSchema()
sales_df.printSchema()


print("Number of rows in customer_df:", customer_df.count())
print("Number of rows in city_df:", city_df.count())
print("Number of rows in sales_df:", sales_df.count())

customer_df.show(5)
city_df.show(5)
sales_df.show()



# Write the DataFrame to MySQL
customer_df.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/DW").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Customer").option("user", "root").option("password", "Chaitanya18").mode("overwrite").save()                                                
# customer_df.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/DW").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Customer").option("user", "root").option("password", "Chaitanya18").mode("overwrite").save()
city_df.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/DW").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "City").option("user", "root").option("password", "Chaitanya18").mode("overwrite").save()
sales_df.writeformat("jdbc").option("url", "jdbc:mysql://localhost:3306/DW").option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "Sales").option("user", "root").option("password", "Chaitanya18").mode("overwrite").save()

# # Create temporary views for SQL querying
# customer_df.createOrReplaceTempView("customer")
# city_df.createOrReplaceTempView("city")
# sales_df.createOrReplaceTempView("sales")

# Creating Date Table from Sales Table
date_df = spark.sql("""
    SELECT DISTINCT
        InvoiceDateKey AS Date,
        WEEKOFYEAR(InvoiceDateKey) AS WeekNumber,
        DAYOFWEEK(InvoiceDateKey) AS DayOfWeek,
        MONTH(InvoiceDateKey) AS Month,
        QUARTER(InvoiceDateKey) AS Quarter,
        YEAR(InvoiceDateKey) AS Year
    FROM Sales
""")

# date_df.createOrReplaceTempView("Date")
date_df.show(5)

spark.stop()