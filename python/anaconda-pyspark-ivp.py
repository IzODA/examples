# Installation Verification Program (IVP)
# Copyright IBM 2017

# izodaIVP.py
# Last updated 10/12/2017
#
# Simple analysis on a VSAM file
# VSAM file contains Client Retention Demo

import sys
from pyspark.sql import SparkSession
import pandas

helpText = \
    '''
    Usage:
        spark-submit anaconda-pyspark-ivp.py [mdsURL] [user] [password]
    '''

if __name__ == "__main__":

    # Check and process parameters
    if len(sys.argv) != 2:
        print(helpText)
        sys.exit(-1)
    else:
        mdssURL = sys.argv[1]

    # Initialize Spark
    spark = SparkSession \
        .builder \
        .appName("izodaIVP") \
        .config("spark.files.overwrite", "true") \
        .getOrCreate()
        

    # Read MDSS Client Retention VSAM file and place that data into a dataframe
    StaffDF = spark.read \
        .format("jdbc") \
        .option("driver", "com.rs.jdbc.dv.DvDriver") \
        .option("url", mdssURL) \
        .option("dbtable", "STAFFVS") \
        .option("user", "") \
        .option("password", "") \
        .load()

    # Group dataframe values by education level
    # Aggregate values on multiple columns
    avgByDeptDF = StaffDF \
	.groupBy("STAFFVS_DATA_DEPT") \
        .avg("STAFFVS_DATA_YRS","STAFFVS_DATA_NAME_L") \
	.orderBy("STAFFVS_DATA_DEPT")

    # Print all results
    avgByDeptDF.show()
    
    # Convert Spark dataframe to pandas dataframe and transpose
    print(avgByDeptDF.toPandas().transpose())

    # Stop Spark Session
    spark.stop()
