# IBM z Open Data Analytics (IzODA)
# Installation Verification Program (IVP)
# Copyright IBM 2017

# izodaIVP.py
# Last updated 08/05/2017
#
# Simple analysis on a VSAM file
# VSAM file contains Client Retention Demo

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pandas

helpText = \
    '''
    Usage:
        spark-submit anaconda-pyspark-ivp.py [mdsURL] [user] [password]
    '''

if __name__ == "__main__":

    # Check and process parameters
    if len(sys.argv) != 4:
        print(helpText)
        sys.exit(-1)
    else:
        mdssURL = sys.argv[1]
        user = sys.argv[2]
        password = sys.argv[3]

    # Initialize Spark
    spark = SparkSession \
        .builder \
        .appName("izodaIVP") \
        .config("spark.files.overwrite", "true") \
        .getOrCreate()

    schema = StructType([
        StructField("CONT_ID", IntegerType()),
        StructField("GENDER", IntegerType()),
        StructField("AGE_YEARS", DoubleType()),
        StructField("HIGHEST_EDU", IntegerType()),
        StructField("ANNUAL_INVEST", DoubleType()),
        StructField("ANNUAL_INCOME", IntegerType()),
        StructField("ACTIVITY_LEVEL", IntegerType()),
        StructField("CHURN", IntegerType()),
    ])

    # Read MDSS Client Retention VSAM file and place that data into a dataframe
    clientIncomeDF = spark.read \
        .format("jdbc") \
        .option("url", mdssURL) \
        .option("dbtable", "CLIENT_RETN") \
        .option("user", user) \
        .option("password", password) \
        .load()

    # Group dataframe values by education level
    # Aggregate values on multiple columns
    avgByEducationDF = clientIncomeDF \
        .groupBy("HIGHEST_EDU") \
        .avg("AGE_YEARS", "ANNUAL_INVEST", "ANNUAL_INCOME") \
        .orderBy("HIGHEST_EDU")

    # Print all results
    avgByEducationDF.show(avgByEducationDF.count())
    
    # Convert Spark dataframe to pandas dataframe and transpose
    print(avgByEducationDF.toPandas().transpose())

    # Stop Spark Session
    spark.stop()
