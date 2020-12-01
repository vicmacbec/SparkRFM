# A quick RFM Analysis on Spark

RFM analysis is a common customers scan in retail and is generally used to create customer groups. RFM is the acronym of Recency, Frequency and Monetary.

Recency, is usually the number of days since the customer make the last purchase. 

<Image>

Frequency is the number of times that a customer make a purchase in a period of time (it could be since the first and the last purchase). 

<Image>

And Monetary stands for the mount of money that the customer spends on your product.

<Image>

Usually, retail volume data is huge and is hard to process it. Spark is an open-source distributed general-purpose cluster-computing framework and is the perfect tool to handle big data.

On this time, I will show you how to make an RFM analysis using the basiscs of Spark. To do so, I will use Databricks Community Edition. 

<img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> You can see the whole notebook from this [repository](https://github.com/vicmacbec/SparkRFM).

## Getting retail data

The **Retail Data Analytics** dataset, shared on [Kaggle](https://www.kaggle.com/manjeetsingh/retaildataset?select=Features+data+set.csv), will be explored using Spark functions.

As I used Databricks Community Edition, it is necesary to load the data first on the enviroment.

### Import functions

The functions that will be used are:

    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, BooleanType, DoubleType, DateType  
    from pyspark.sql.functions import to_date, col, datediff, lit  
    import pyspark.sql.functions as sf

Basically, pyspark.sql.types imported functions are used to declared schemas; pyspark.sql.types imported functions are used to easilly operate with the features; and pyspark.sql.functions is to easily use descriptive functions.

### Reading data

First of all, it is necessary to explore the file system where data was loaded, to do so, lets list the file store.

    display(dbutils.fs.ls("FileStore/tables"))

### Joning data

## Quick data exploration

## RFM Analysis

### Recency

### Frequency

### Monetary

## Conclusions

### Future steps

- Create groups of the customers using an ML algorithm like K-means or PAM.