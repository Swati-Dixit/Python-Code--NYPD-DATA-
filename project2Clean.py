#Cloud Computing - Project 2 - data cleaning in Spark
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *

conf = SparkConf().setAppName("CCProject2 - GUPTASW")
sc = SparkContext(conf = conf)

#Creating spark session
spark = SparkSession.builder.appName("CC Proj 2- GUPTASW").config("spark.some.config.option", "some-value").getOrCreate()


#creating data frame
dataFrame = spark.read.option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .csv("/user/data/nypd/NYPD_Motor_Vehicle_WithHeader.txt")

#selecting desired fields
new_dataFrame = dataFrame.select(dataFrame['#DATE'], dataFrame['BOROUGH'], dataFrame['ZIP Code'],dataFrame['NUMBER OF PERSONS INJURED'], dataFrame['NUMBER OF PERSONS KILLED'], dataFrame['NUMBER OF PEDESTRIANS INJURED'],dataFrame['NUMBER OF PEDESTRIANS KILLED'], dataFrame['NUMBER OF CYCLIST INJURED'],dataFrame['NUMBER OF CYCLIST KILLED'],dataFrame['NUMBER OF MOTORIST INJURED'],dataFrame['NUMBER OF MOTORIST KILLED'],dataFrame['VEHICLE TYPE CODE 1'])

dataFrame1 = new_dataFrame.na.drop()
dataFrame1.repartition(1).write.format("csv").option("header", "true").save("/user/guptasw/project/cleaned.txt")
