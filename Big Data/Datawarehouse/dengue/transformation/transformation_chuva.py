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

location_bronze = '/FileStore/bronze/dados_degue/chuvas'


# COMMAND ----------

# MAGIC %md
# MAGIC #### Leiura camada bronze em dataframe spark

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformações camada silver 

# COMMAND ----------



# COMMAND ----------

path_silver_chuva= '/FileStore/silver/dados_degue/chuvas'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Processo de Merge/Update para camada Silver

# COMMAND ----------



# COMMAND ----------

