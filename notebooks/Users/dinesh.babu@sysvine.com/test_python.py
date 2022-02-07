# Databricks notebook source
# MAGIC %sql
# MAGIC select * from product_test where price =(select min(price) from product_test)

# COMMAND ----------

minPriceProduct = spark.sql("select * from product_test where price =(select min(price) from product_test)")

minPriceProduct.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("/FileStore/tables/minPrice")

# COMMAND ----------

display(minPriceProduct)

# COMMAND ----------

# MAGIC %fs
# MAGIC rm ./FileStore/tables/etl_alloc_outlet_allocations.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC ls ./FileStore/tables

# COMMAND ----------

import-stagecheckFileExist = spark.read.format("csv").option("header","true").load("/FileStore/tables/minPrice/part-00000-tid-4192029505969046231-ebbe5bf4-538d-4c7f-a4a2-00442f346c3a-96-1-c000.csv")

# COMMAND ----------

display(checkFileExist)

# COMMAND ----------

spark.read.format("csv").option("header","true").load("/FileStore/tables/minPrice/part-00000-tid-4192029505969046231-ebbe5bf4-538d-4c7f-a4a2-00442f346c3a-96-1-c000.csv").createOrReplaceTempView("min_price_product")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from min_price_product

# COMMAND ----------

checkFileExist =  checkFileExist.select("*").limit(5)

checkFileExist.createOrReplaceTempView("first_five_product")
checkFileExist = spark.sql("select * from min_price_product UNION ALL select * from first_five_product ")

# COMMAND ----------

from pyspark.sql.functions import lit, col, create_map

testdf = checkFileExist.select("*").filter(col("available_size").isNotNull())
print(testdf.count())

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import os
from pyspark.sql.functions import *
import datetime

df = spark.read.format("text").option("ignoreLeadingWhiteSpace",True).option("ignoreTrailingWhiteSpace",True).load("/FileStore/shared_uploads/uma.rajkumar@sysvine.com/BVN.txt")

data = df.withColumn('pos_id',substring('value', 0,20).cast('STRING')).withColumn('ppmonth',substring('value', 21,10).cast('STRING'))

# COMMAND ----------

data.display()

# COMMAND ----------

dbutils.widgets.text("FileName", "")
FileName = dbutils.widgets.get("FileName")

dbutils.widgets.text("SrcPath", "")
SrcPath = dbutils.widgets.get("SrcPath")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select DATE_FORMAT(ADDED_DATE, '%m/%d/%Y %H:%i') from etl_alloc_bp_universe LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC update etl_alloc_bp_universe set ADDED_DATE = FROM_UNIXTIME(ADDED_DATE), UPDATED_DATE = FROM_UNIXTIME(UPDATED_DATE)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS test_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS test_db CASCADE;

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/amazon_com.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

permanent_table_name = "amazon_com_test1"

df.write.saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC use test_db;
# MAGIC select * from amazon_test where product_name='Wacoal Embrace Lace Bikini Panty';

# COMMAND ----------

# MAGIC %sql
# MAGIC update amazon_test set product_name='test product' where product_name='Wacoal Embrace Lace Bikini Panty';

# COMMAND ----------

# MAGIC %sql
# MAGIC use test_db;
# MAGIC ALTER TABLE amazon_test add column test_column TIMESTAMP; 

# COMMAND ----------

# MAGIC %sql
# MAGIC use test_db;
# MAGIC select * from amazon_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC update amazon_test set test_column=1635945745;

# COMMAND ----------

# MAGIC %sql
# MAGIC use default;
# MAGIC describe test_unmanage_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC use test_db;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe amazon_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE amazon_test CHANGE COLUMN product_name SET NOT NULL;

# COMMAND ----------

