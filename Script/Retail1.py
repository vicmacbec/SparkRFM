# Databricks notebook source
# MAGIC %md
# MAGIC # Project
# MAGIC 
# MAGIC On this notebook,the **Retail Data Analytics** dataset, shared on [Kaggle](https://www.kaggle.com/manjeetsingh/retaildataset?select=Features+data+set.csv), will be explored using Spark functions.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, BooleanType, DoubleType, DateType
from pyspark.sql.functions import to_date, col, datediff, lit
import pyspark.sql.functions as sf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading csv's:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring file system

# COMMAND ----------

#display(dbutils.fs.ls("mnt/etlp1a-vicmacbec1@hotmail.com-si/retail-org"))
display(dbutils.fs.ls("FileStore/tables"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Features data set.csv

# COMMAND ----------

# MAGIC %fs head FileStore/tables/Features_data_set.csv

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Features_data_set.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
features = (spark.read.format(file_type)
            .option("inferSchema", infer_schema)
            .option("header", first_row_is_header)
            .option("sep", delimiter)
            .load(file_location))

display(features)

# COMMAND ----------

features.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC It is observed that the features Date, MarkDown*, CPI and Unemployment are incorrectly inferred so the schema is declared.

# COMMAND ----------

featuresSchema = StructType([
  StructField("Store", IntegerType(), True), 
  StructField("Date", StringType(), True),
  StructField("Temperature", DoubleType(), True),
  StructField("Fuel_Price", DoubleType(), True),
  StructField("MarkDown1", DoubleType(), True),
  StructField("MarkDown2", DoubleType(), True),
  StructField("MarkDown3", DoubleType(), True),
  StructField("MarkDown4", DoubleType(), True),
  StructField("MarkDown5", DoubleType(), True),
  StructField("CPI", DoubleType(), True),
  StructField("Unemployment", DoubleType(), True),
  StructField("IsHoliday", BooleanType(), True)
])

features2 = (spark.read
             .format(file_type)
             .option("header", first_row_is_header)
             .option("sep", delimiter)
             .schema(featuresSchema)
             .load(file_location))

display(features2)

# COMMAND ----------

# MAGIC %md
# MAGIC Changing Date type (String -> Date)

# COMMAND ----------

features3 = features2.withColumn("Date", to_date(col("Date"),"dd/MM/yyyy"))
#changedTypedf = joindf.withColumn("show", joindf["show"].cast(DoubleType()))
display(features3)

# COMMAND ----------

features3.printSchema()

# COMMAND ----------

features3.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sale data-set.csv

# COMMAND ----------

# MAGIC %fs head FileStore/tables/sales_data_set.csv

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/sales_data_set.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
sales = (spark.read.format(file_type)
         .option("inferSchema", infer_schema)
         .option("header", first_row_is_header)
         .option("sep", delimiter)
         .load(file_location))

display(sales)

# COMMAND ----------

sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC It is observed that the feature Date is incorrectly inferred so the schema is declared.

# COMMAND ----------

salesSchema = StructType([
  StructField("Store", IntegerType(), True), 
  StructField("Dept", IntegerType(), True),
  StructField("Date", StringType(), True),
  StructField("Weekly_Sales", DoubleType(), True),
  StructField("IsHoliday", BooleanType(), True)
])

sales2 = (spark.read
          .format(file_type)
          .option("header", first_row_is_header)
          .option("sep", delimiter)
          .schema(salesSchema)
          .load(file_location))

display(sales2)

# COMMAND ----------

# MAGIC %md
# MAGIC Changing Date type (String -> Date)

# COMMAND ----------

sales3 = sales2.withColumn("Date", to_date(col("Date"),"dd/MM/yyyy"))
#changedTypedf = joindf.withColumn("show", joindf["show"].cast(DoubleType()))
display(sales3)

# COMMAND ----------

sales3.printSchema()

# COMMAND ----------

sales3.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### stores data-set.csv

# COMMAND ----------

# MAGIC %fs head FileStore/tables/stores_data_set.csv

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/stores_data_set.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
stores = (spark.read.format(file_type)
          .option("inferSchema", infer_schema)
          .option("header", first_row_is_header)
          .option("sep", delimiter)
          .load(file_location))

display(stores)

# COMMAND ----------

stores.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC It is observed that the features are correctly inferred.

# COMMAND ----------

stores.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joinning datasets

# COMMAND ----------

# features3, sales3, stores
featuresSales = features3.join(sales3, on=['Store','Date','IsHoliday'], how='full') # Store, Date, IsHoliday

display(featuresSales)

# COMMAND ----------

featuresSales.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Given the counts of the records in the features3 (8,190) and sales3 (421,570) tables, it is observed that joining these tables yields (423,325) records, which indicates that there are records in each table that do not have the id corresponding to each other.

# COMMAND ----------

featuresSalesStores = featuresSales.join(stores, on=['Store'], how='full') # Store

display(featuresSalesStores)

# COMMAND ----------

featuresSalesStores.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Exploring data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Showing joined table

# COMMAND ----------

display(featuresSalesStores)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descriptive statistic analysis

# COMMAND ----------

featuresSalesStores.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Making some groupings

# COMMAND ----------

# MAGIC %md
# MAGIC #### Groupby type

# COMMAND ----------

(featuresSalesStores
 .groupBy("Type")
 .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales_Sum'))
 .withColumn("Weekly_Sales_Sum", sf.round(col("Weekly_Sales_Sum"), 2))
 .sort('Type')
 .display())

# COMMAND ----------

# featuresSalesStores.groupBy("type").agg({'Weekly_Sales':'avg'}).display()
(featuresSalesStores
 .groupBy("Type")
 .agg(sf.avg('Weekly_Sales').alias('Weekly_Sales_Avg'))
 .withColumn("Weekly_Sales_Avg", sf.round(col("Weekly_Sales_Avg"), 2))
 .sort('Type')
 .display())

# COMMAND ----------

(featuresSalesStores
 .groupBy("Type")
 .count()
 .sort('Type')
 .display())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Groupby type & Date

# COMMAND ----------

(featuresSalesStores
 .groupBy(['Date','Type'])
 .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales_Sum'))
 .withColumn('Weekly_Sales_Sum', sf.round(col('Weekly_Sales_Sum'), 2))
 .sort('Date','Type')
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Groupby type & dept

# COMMAND ----------

(featuresSalesStores
 .groupBy(['Dept','Type'])
 .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales_Sum'))
 .withColumn('Weekly_Sales_Sum', sf.round(col('Weekly_Sales_Sum'), 2))
 .sort('Dept','Type')
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plots

# COMMAND ----------

# MAGIC %md
# MAGIC #### Type ~ Date

# COMMAND ----------

(featuresSalesStores
 .groupBy(['Date','Type'])
 .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales_Sum'))
 .withColumn('Weekly_Sales_Sum', sf.round(col('Weekly_Sales_Sum'), 2))
 .sort('Date','Type')
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Weekly Sales ~ Date
# MAGIC 
# MAGIC Analyzing weekly apartment sales. It is observed that the sales of department 2 are constant throughout the year and are the highest, however departments 4 and 7 are the ones that sell the most in December.

# COMMAND ----------

(featuresSalesStores
 .groupBy(['Date','Dept'])
 .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales_Sum'))
 .withColumn('Weekly_Sales_Sum', sf.round(col('Weekly_Sales_Sum'), 2))
 .sort('Date','Dept')
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Boxplot

# COMMAND ----------

# MAGIC %md
# MAGIC Analyzing the weekly sales of each department. It is observed that department 16 has a large variation of weekly sales.

# COMMAND ----------

(featuresSalesStores
 .groupBy(['Date','Dept'])
 .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales_Sum'))
 .withColumn('Weekly_Sales_Sum', sf.round(col('Weekly_Sales_Sum'), 2))
 .sort('Date','Dept')
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFM Analysis (Recency, Frequency, Monetary)
# MAGIC 
# MAGIC First, lets select the necessary features:
# MAGIC - Store
# MAGIC - Date
# MAGIC - Weekly_Sales
# MAGIC 
# MAGIC Then, lets drop na's and group by Date and Store making the Weekly_Sales sum.

# COMMAND ----------

RFM0 = (featuresSalesStores
       .select('Store', 'Date', 'Weekly_Sales')
       .dropna('any')
       .groupby(['Store','Date'])
       .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales'))
       .withColumn('Weekly_Sales', sf.round(col('Weekly_Sales'), 2))
       )

RFM0.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recency
# MAGIC 
# MAGIC First, lets take the last date reported.

# COMMAND ----------

max_date = RFM0.agg(sf.max('Date'))
max_date.display()

maxDate = max_date.groupBy('max(Date)').max().collect()[0] # Para tomar el valor del DataFrame

# COMMAND ----------

max_date.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Then the last date recorded by each store is reviewed. In this case, it is observed that all stores have bought on the same date as the global maximum date.

# COMMAND ----------

storesMaxDate = RFM0.groupby(['Store']).agg(sf.max('Date'))
display(storesMaxDate)

# COMMAND ----------

storesMaxDate.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Obtaining the difference between the global maximum date and the maximum date of each store:

# COMMAND ----------

r = (RFM0
     .join(storesMaxDate, on = 'Store')
     .withColumn('Recency', datediff(to_date(lit(maxDate[0])), col('max(Date)')))
    )

display(r)
#recency.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Frequency
# MAGIC 
# MAGIC The frequency of purchases that occurred in the given period is obtained. In this case, it is observed how all the stores had sales every day.

# COMMAND ----------

f = (RFM0
    .groupby(['Store'])
    .count()
    .withColumn('Frequency',col('count'))
    )

f.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Monetary

# COMMAND ----------

m = (RFM0
    .groupby('Store')
    .agg(sf.sum('Weekly_Sales').alias('Monetary'))
    .withColumn('Monetary', sf.round(col('Monetary'), 2))
    )

m.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RFM

# COMMAND ----------

# MAGIC %md
# MAGIC Joining r, f, m DataFrames with all the stores.

# COMMAND ----------

RFM1 = (r.select('Store', 'Recency').distinct()
       .join(f.select('Store', 'Frequency'), on = 'Store')
       .join(m, on = 'Store')
       )

RFM1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving data

# COMMAND ----------

#%sh
#pwd
#ls 
display(dbutils.fs.ls("FileStore/tables"))

# COMMAND ----------

workingDir = "FileStore/tables"
targetPath = f"{workingDir}/RFM"

RFM1.write.mode("OVERWRITE").parquet(targetPath)

# COMMAND ----------

display(dbutils.fs.ls(targetPath))
