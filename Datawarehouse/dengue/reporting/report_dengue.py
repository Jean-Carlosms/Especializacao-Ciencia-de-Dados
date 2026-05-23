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

# MAGIC %md
# MAGIC #### Caminhos para a camada silver

# COMMAND ----------

path_silver_dengue= '/FileStore/silver/dados_degue/casos_dengue'
path_silver_chuva= '/FileStore/silver/dados_degue/chuvas'


# COMMAND ----------

# MAGIC %md
# MAGIC #### Leitura da dos dados em dataframes

# COMMAND ----------

df_dengue = spark.read.format('delta').load(path_silver_dengue)

# COMMAND ----------

df_chuva = spark.read.format('delta').load(path_silver_chuva)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criação do banco de dados dengue_chuvas

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Analise soma casos dengue agrupado por 'ano','mes','estado','cidade'

# COMMAND ----------

df_dengue.groupBy('ano','mes','estado','cidade').agg(
    f.sum('quantidade_casos').alias('soma_casos_dengue')
    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analise soma quantidade de chuva agrupado por 'ano','mes','estado'

# COMMAND ----------

df_media_chuva = df_chuva.groupBy('ano','mes','estado').agg(
    f.sum('mm').alias('soma_chuva')
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analise analise_chuvas_dengue 'ano','mes','estado'

# COMMAND ----------

df_media_dengue =  df_dengue.groupBy('ano','mes','estado').agg(
    f.sum('quantidade_casos').alias('soma_casos_degue')
)  #.filter((f.col('estado') == 'SP') & (f.col('ano') == '2015') & (f.col('mes') == '1'))

# COMMAND ----------

df_media_chuva = df_chuva.groupBy('ano','mes','estado').agg(
    f.abs(f.sum('mm')).alias('soma_chuva')
)#.filter((f.col('estado') == 'SP') & (f.col('ano') == '2015') & (f.col('mes') == '1'))

# COMMAND ----------

condicao = (df_media_dengue.ano == df_media_chuva.ano) & (df_media_dengue.mes == df_media_chuva.mes) & (df_media_dengue.estado == df_media_chuva.estado)
df_media_dengue.join(df_media_chuva,condicao,'inner').drop(df_media_dengue.ano, df_media_dengue.mes, df_media_dengue.estado).withColumn('casos_Dengue/ChuvaAcum',f.col('soma_casos_degue')/f.col('soma_chuva')).display()