from pyspark.sql import SparkSession

import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName("JSON file").getOrCreate()
product_json = sys.argv[1]
df = spark.read.json(product_json)
columns_to_drop = ['ThumbNailPhoto', 'ThumbnailPhotoFileName']
df = df.drop(*columns_to_drop)
# df.printSchema()


df_clean = df.filter("_corrupt_record IS NULL")

# Write the modified data to a new JSON file
output_json_path = "NewSalesLT_Product_20200716.json"
df_clean.write.mode("overwrite").json(output_json_path)

# Stop the Spark session
spark.stop()