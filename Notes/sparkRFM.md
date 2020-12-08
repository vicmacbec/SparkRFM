# A quick RFM Analysis on Spark

RFM analysis is a common customers scan in retail and is generally used to create customer groups. RFM is the acronym of Recency, Frequency and Monetary.

Recency, is usually the number of days since the customer make the last purchase. 

<Image>

Frequency is the number of times that a customer make a purchase in a period of time (it could be since the first and the last purchase). 

<Image>

And Monetary stands for the mount of money that the customer spends on your product.

<Image>

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

Basically, pyspark.sql.types imported functions are used to declared schemas; pyspark.sql.types imported functions are used to easilly operate with the features; and pyspark.sql.functions is to easily use descriptive functions.

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

As you can see, read a file is so easy. With the `display` command, you can see the content of the read file.

### Joning data

## Quick data exploration

## RFM Analysis

### Recency

### Frequency

### Monetary

## Conclusions

### Future steps

- Create groups of the customers using an ML algorithm like K-means or PAM.