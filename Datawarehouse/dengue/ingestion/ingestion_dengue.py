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

def create_deltaTable_insert_update_rows(spark:SparkSession,columns:list, location:str,merge_condition:str,df:DataFrame):
    if (DeltaTable.isDeltaTable(spark, location)):
        print('tabela delta existente')
        deltaTable = DeltaTable.forPath(spark, location)
        deltaTable.alias('tgt') \
            .merge(
                df.alias('src'),
                merge_condition
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        print('tabela delta inexistente')    
        DeltaTable \
            .create(spark) \
            .addColumns(columns) \
            .location(location) \
            .execute()
        deltaTable = DeltaTable.forPath(spark, location)
        deltaTable.alias('tgt') \
            .merge(
                df.alias('src'),
                merge_condition
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()


# COMMAND ----------

spark = create_spark_session()

# COMMAND ----------

path = '/FileStore/transient/dados_degue/casos_dengue/'
df_dengue = spark.read.format('csv').option('header',True).option('Sep','|').option('InferSchema', True).load(path)

# COMMAND ----------

location = '/FileStore/bronze/dados_degue/casos_dengue'
merge_condition = "tgt.data_iniSE = src.data_iniSE and tgt.ibge_code = src.ibge_code"

columns = [
    StructField('id', IntegerType(), True),
    StructField('data_iniSE', DateType(), True),
    StructField('casos', DoubleType(), True),
    StructField('ibge_code', IntegerType(), True),
    StructField('cidade', StringType(), True),
    StructField('uf', StringType(), True),
    StructField('cep', StringType(), True),
    StructField('latitude', DoubleType(), True),
    StructField('longitude', DoubleType(), True)
]
create_deltaTable_insert_update_rows(spark,columns, location,merge_condition,df_dengue)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

