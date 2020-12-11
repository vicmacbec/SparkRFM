# A quick RFM Analysis on Spark
## First steps using Spark

## RFM introduction

RFM analysis is a common customers scan in retail and is generally used to create customer groups. RFM is the acronym of Recency, Frequency and Monetary.

Recency, is usually the number of days since the customer make the last purchase. 

<img alt="Recency" title="Recency" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/Recency.png"/>

Frequency is the number of times that a customer make a purchase in a period of time. 

<img alt="Frequency" title="Frequency" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/Frequency.png"/>

And Monetary stands for the mount of money that the customer spends on your product.

<img alt="Monetary" title="Monetary" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/Monetary.png"/>

Usually, retail volume data is huge and is hard to process it. Spark is an open-source distributed general-purpose cluster-computing framework and is the perfect tool to handle big data.

On this time, I will show you how to make an RFM analysis using the basics of Spark (clearly explained if it is your first time on Spark). To do so, I will use Databricks Community Edition. 

<img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can see the whole notebook from this [repository](https://github.com/vicmacbec/SparkRFM).

## Getting retail data

The **Retail Data Analytics** dataset, shared on [Kaggle](https://www.kaggle.com/manjeetsingh/retaildataset?select=Features+data+set.csv), will be explored using Spark functions.

First, as I used Databricks Community Edition, is necesary to load the data on the enviroment.

### Import functions

The functions that will be used are:

    import pyspark.sql.functions as sf
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, BooleanType, DoubleType, DateType  
    from pyspark.sql.functions import to_date, col, datediff, lit

Basically, `pyspark.sql.types` imported functions are used to declared schemas; `pyspark.sql.types` imported functions are used to easilly operate with the features; and `pyspark.sql.functions` is to easily use descriptive functions.

### Reading data

First of all, it is necessary to explore the file system, where data was loaded. To do so, lets list the file store. Using Databricks, it is possible using:

    display(dbutils.fs.ls("FileStore/tables"))

The result most be something like

<img alt="fileStore" title="File Store" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/fileStore.png"/>

To know how to read the files, it is important to make a small visualization, to do so, it is possible to manipulates the Databricks filesystem (DBFS) from the console using the `%fs` shorthand or using the module dbutils.fs, you can learn more [here](https://docs.databricks.com/_static/notebooks/dbutils.html).

To exemplify, lets use the `%fs` shorthand.

    %fs head FileStore/tables/Features_data_set.csv

with output:

<img alt="csvHead" title="csv head" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/csvHead.png"/>

and repeat this process with all the other files.

Once we observed that if the files are comma separated or if they have headers, we proceed to infer the schema.

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

As you can see, read a file is so easy. With the `display()` function, you can see the content of the read file.

<img alt="InferedSchema" title="Infered Schema" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/InferedSchema.png"/>

It is notable that the schema was not correctly inferred. The method `printSchema()` allow see the type of each column.

    features.printSchema()

<img alt="printSchema" title="Print Schema" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/printShema.png"/>

Now, we confirm how the shcema was not correctly inferred. The features *Date*, *MarkDown**, *CPI* and *Unemployment* are incorrectly inferred so the schema now is declared. 

To do so, let's say to Spark the correct type of each column and read the file again in a new variable (it is important because the variables in Saprk are immutable).

As work with dates in Spark is a little complicated, the date was declared as string and then it will be converted as date.

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

<img alt="declaredSchema" title="Declared Schema" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/declaredSchema.png"/>

Changing the type of the column *Date*:

    features3 = features2.withColumn("Date", to_date(col("Date"),"dd/MM/yyyy"))

    features3.printSchema()

<img alt="declaredSchema2" title="Declared Schema 2" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/declaredSchema2.png"/>

As it is remarkable, the types and the values of each column now are correclty. This process most also be repeated with the files **sales** and **stores**, to get the following dataframes:

<img alt="sales" title="Sales" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/sales.png"/>

<img alt="stores" title="Stores" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/stores.png"/>

Finally, it is for interest know the number of registers of each table with the following commands:

    features3.count()
    sales3.count()
    stores.count()

They gives 8190, 421570 and 45 registers respectively.

### Joinning dataframes

Now, lets join our three dataframes. First, take the **features3** and **sales3** dataframes and full join them on *Store*, *Date* and *IsHoliday* columns; and the result be full joined with **stores** dataframe on *Stores* column.

    featuresSales = features3.join(sales3, on=['Store','Date','IsHoliday'], how='full')
    featuresSalesStores = featuresSales.join(stores, on=['Store'], how='full')

Remember to use the `display()` function to see if the result is what you expected. On this time, it is.

<img alt="dfJoined" title="DF Joined" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/dfJoined.png"/>

Given the counts of the records in the *features3* (8,190) and *sales3* (421,570) tables, it is observed that joining these tables yields (423,325) records, which indicates that there are records in each table that do not have the id corresponding to each other.

## Quick data exploration

To make an quick data exploration, lets summarise the data using the *describe()* method.

    featuresSalesStores.describe().display()

<img alt="describe" title="Describe" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/describe.png"/>

In order to do not make this article so extensive, lets pass to the RFM analysis. In other article we can have fun making an EDA.

## RFM Analysis (Recency, Frequency, Monetary)

First, lets select the necessary features:
- Store
- Date
- Weekly_Sales

Then, lets drop na's and group by Date and Store making the Weekly_Sales sum.

    RFM0 = (featuresSalesStores
       .select('Store', 'Date', 'Weekly_Sales')
       .dropna('any')
       .groupby(['Store','Date'])
       .agg(sf.sum('Weekly_Sales').alias('Weekly_Sales'))
       .withColumn('Weekly_Sales', sf.round(col('Weekly_Sales'), 2))
       )

<img alt="RFM" title="RFM" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/RFM.png"/>

### Recency

First, lets take the last date reported, which is *2012-10-06*.

    max_date = RFM0.agg(sf.max('Date'))

    maxDate = max_date.groupBy('max(Date)').max().collect()[0]

Then the last date recorded by each store is reviewed. In this case, it is observed that all stores have bought on the same date as the global maximum date.

    storesMaxDate = RFM0.groupby(['Store']).agg(sf.max('Date'))

<img alt="Recency dates" title="Recency dates" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/Rdates.png"/>

Obtaining the difference between the global maximum date and the maximum date of each store:

    r = (RFM0
     .join(storesMaxDate, on = 'Store')
     .withColumn('Recency', datediff(to_date(lit(maxDate[0])), col('max(Date)')))
    )

<img alt="Recency" title="R" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/R.png"/>    

### Frequency

The frequency of purchases that occurred in the given period is obtained. In this case, it is observed how all the stores had sales every day.

    f = (RFM0
        .groupby(['Store'])
        .count()
        .withColumn('Frequency',col('count'))
        )

<img alt="Frequency" title="F" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/F.png"/>

### Monetary

Getting weekly sales for each store:

    m = (RFM0
        .groupby('Store')
        .agg(sf.sum('Weekly_Sales').alias('Monetary'))
        .withColumn('Monetary', sf.round(col('Monetary'), 2))
        )

<img alt="Monetary" title="M" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/M.png"/>

### RFM

Joining r, f, m DataFrames with all the stores.

    RFM1 = (r.select('Store', 'Recency').distinct()
       .join(f.select('Store', 'Frequency'), on = 'Store')
       .join(m, on = 'Store')
       )

<img alt="RFM" title="RFM" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/RFM2.png"/>

#### Saving data

Finally, we are going to save the information in parquet format, since it is the best compression and reading option for future occasions.

Lets save it on the same path were we save the retail data.

    workingDir = "FileStore/tables"
    targetPath = f"{workingDir}/RFM"

    RFM1.write.mode("OVERWRITE").parquet(targetPath)

    display(dbutils.fs.ls(targetPath))

<img alt="Parquet" title="Parquet" style="vertical-align: text-bottom; position: relative;" src="https://raw.githubusercontent.com/vicmacbec/SparkRFM/main/Images/parquet.png"/>

## Conclusions

On this article, we learn:
- to import data to spark, 
- to read files,
- to make some transformation to the data
- to do an RFM analysis
- to save data.

### Future steps

- Create customers groups using an Machine Learning algorithm like K-means or PAM using the RFM analysis.