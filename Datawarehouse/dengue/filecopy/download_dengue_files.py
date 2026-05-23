# Databricks notebook source
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType, DateType
import sys
import os
from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from delta.tables import *
import io
import json

# COMMAND ----------

def create_spark_session():
    return SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport()\
        .getOrCreate()

# COMMAND ----------

spark = create_spark_session()

# COMMAND ----------

try:
    dbutils.fs.rm('dbfs:/FileStore/transient/dados_degue/',True)
    dbutils.fs.mkdirs('dbfs:/FileStore/transient/dados_degue/')
    dbutils.fs.mkdirs('dbfs:/FileStore/transient/dados_degue/casos_dengue/')
    dbutils.fs.mkdirs('dbfs:/FileStore/transient/dados_degue/chuvas')
except:
    print("Erro")
finally:
    print("Fim")


# COMMAND ----------

def download_Copy_File_dengue() :
    url = 'https://github.com/jader-lima/pyspark_introducao/raw/dev/pyspark/datalake/transient/dengue/dados-dengue.tgz -P /tmp/'
    ! wget $url
    origin_chuva_2015 = "file:/tmp/chuva_2015.csv"
    origin_chuva_2016 = "file:/tmp/chuva_2016.csv"
    origin_chuva_2017 = "file:/tmp/chuva_2017.csv"
    origin_chuva_2018 = "file:/tmp/chuva_2018.csv"
    origin_chuva_2019 = "file:/tmp/chuva_2019.csv"
    origin_dengue = "file:/tmp/casos_dengue.txt"
    sink_chuva_2015 = "/FileStore/transient/dados_degue/chuvas/chuva_2015.csv"
    sink_chuva_2016 = "/FileStore/transient/dados_degue/chuvas/chuva_2016.csv"
    sink_chuva_2017 = "/FileStore/transient/dados_degue/chuvas/chuva_2017.csv"
    sink_chuva_2018 = "/FileStore/transient/dados_degue/chuvas/chuva_2018.csv"
    sink_chuva_2019 = "/FileStore/transient/dados_degue/chuvas/chuva_2019.csv"
    sink_dengue = "/FileStore/transient/dados_degue/casos_dengue/casos_dengue.csv"
    ! tar -zxf /tmp/dados-dengue.tgz  --directory /tmp/
    dbutils.fs.cp(origin_chuva_2015, sink_chuva_2015)   
    dbutils.fs.cp(origin_chuva_2016, sink_chuva_2016)  
    dbutils.fs.cp(origin_chuva_2017, sink_chuva_2017)  
    dbutils.fs.cp(origin_chuva_2018, sink_chuva_2018)  
    dbutils.fs.cp(origin_chuva_2019, sink_chuva_2019) 
    dbutils.fs.cp(origin_dengue, sink_dengue)
    !rm /tmp/dados-dengue.tgz
    !rm /tmp/chuva_2015.csv
    !rm /tmp/chuva_2016.csv
    !rm /tmp/chuva_2017.csv
    !rm /tmp/chuva_2018.csv
    !rm /tmp/chuva_2019.csv
    !rm /tmp/casos_dengue.txt

# COMMAND ----------

download_Copy_File_dengue()

# COMMAND ----------

