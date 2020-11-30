# Databricks notebook source
# MAGIC %md
# MAGIC # Proyecto
# MAGIC 
# MAGIC En este notebook se explorarán los datos del dataset de **Retail Data Analytics** propuesta en [Kaggle](https://www.kaggle.com/manjeetsingh/retaildataset?select=Features+data+set.csv) mediante el uso de funciones de Spark.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, BooleanType, DoubleType, DateType
from pyspark.sql.functions import to_date, col, datediff, lit
import pyspark.sql.functions as sf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leyendo csv's:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explorando file system

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
# MAGIC Se observa que Date, MarkDown*, CPI y Unemployment están mal inferidos, por lo que se procede a declarar el esquema.

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
# MAGIC Cambiando el tipo de Date (String -> Date)

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
# MAGIC Se observa que Date está mal inferido, por lo que se procede a declarar el esquema.

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
# MAGIC Cambiando el tipo de Date (String -> Date)

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
# MAGIC Se observa que se inferió correctamente el esquema.

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
# MAGIC Dados los conteos de los registros de las tablas features3 (8,190) y sales3 (421,570), se observa que al hacer outerjoin de estas tablas, se obtienen (423,325) registros, lo que indica que hay registros en cada tabla que no tengan el id correspondiente entre sí.

# COMMAND ----------

featuresSalesStores = featuresSales.join(stores, on=['Store'], how='full') # Store

display(featuresSalesStores)

# COMMAND ----------

featuresSalesStores.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Explorando data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrando tabla

# COMMAND ----------

display(featuresSalesStores)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análisis estadístico descriptivo

# COMMAND ----------

featuresSalesStores.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Realizando algunas agrupaciones

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
# MAGIC Analizando ventas semanales de departamentos. Se observa que las ventas del departamento 2 son constantes en el año y son las más altas, sin embargo los departamentos 4 y 7 son los que más venden en diciembre.

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
# MAGIC Analizando las ventas semanales por cada departamento. Se observa que el departamento 16 tiene una gran varianza de ventas semanales.

# COMMAND ----------

(featuresSalesStores
 .groupBy(['Date','Dept'])
 .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales_Sum'))
 .withColumn('Weekly_Sales_Sum', sf.round(col('Weekly_Sales_Sum'), 2))
 .sort('Date','Dept')
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ## RFM Análisis (Recency, Frequency, Monetary)
# MAGIC 
# MAGIC Primero hay que seleccionar la información de interes:
# MAGIC - Store
# MAGIC - Date
# MAGIC - Weekly_Sales
# MAGIC 
# MAGIC Después hay que quitar nulos y agrupar los datos por Date y Store  y hacer la suma de Weekly_Sales.

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
# MAGIC Primero se obtiene la última fecha a analizar, en este caso será la útlima reportada.

# COMMAND ----------

max_date = RFM0.agg(sf.max('Date'))
max_date.display()

maxDate = max_date.groupBy('max(Date)').max().collect()[0] # Para tomar el valor del DataFrame

# COMMAND ----------

max_date.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Luego se revisa la última fecha registrada por cada tienda. En este caso se observa que todas las tiendas han comprado en la misma fecha que la fecha máxima global.

# COMMAND ----------

storesMaxDate = RFM0.groupby(['Store']).agg(sf.max('Date'))
display(storesMaxDate)

# COMMAND ----------

storesMaxDate.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Obteniendo la diferencia entre la máxima fecha global y la máxima fecha de cada tienda:

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
# MAGIC Se obtiene la frecuencia de compras que hubo en el periodo dado. En este caso se observa como todas las tiendas tuvieron ventas todos los días.

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
# MAGIC Juntando los DataFrames r, f, m con las tiendas.

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
