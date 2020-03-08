#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark.sql import HiveContext, DataFrame
from pyspark.sql import functions
from datetime import datetime
import sys
import pyspark.sql.functions as f
import pyspark as p
import unicodedata
from functools import reduce

conf = SparkConf().setAppName('Teste_nasa')
sc = SparkContext(conf=conf)

hc = HiveContext(sc)

#Patch
pathHiveIn = "CAMINHO NO DE ENTRADA DO ARQUIVO NO HDFS"
pathHiveOut = "CAMINHO DE SAIDA DO ARQUIVO NO HDFS"
#Name file Input
nameFileIn1 = "NASA_access_log_Aug95"
nameFileIn2 = "NASA_access_log_Jul95"
#Name file output
nameFileIn1Out = "NASA_access_log_Aug95_Out-"
nameFileIn2Out = "NASA_access_log_Jul95_Out-"

file_name1 = pathHiveIn+nameFileIn1
file_name2 = pathHiveIn+nameFileIn2
file_name1_out = pathHiveIn+nameFileIn1Out
file_name2_out = pathHiveIn+nameFileIn2Out

#Escolha qual file a ser utilziado.
# Aug95 ou Jul95
env = "Aug95"
file_name_in = ""
file_name_out = ""

if (env == 'Aug95'):
    file_name_in = file_name1
    file_name_out = file_name1_out
elif (env == 'Jul95'):
    file_name_in = file_name2
    file_name_out = file_name2_out
else:
    print("Opção inválida")

#Carregando arquivo
print("Carregando o arquivo do caminho " + file_name_in)
file_path = (file_name_in)
hdfs_lines = sc.textFile(file_path, minPartitions=1)
print("Criando dataframe dp "  + file_name_in)
df_full = hdfs_lines.map(lambda s: s.replace('|','__'))
df_full = df_full.map(lambda s: s.replace('" HTTP', 'HTTP'))
df_full = df_full.map(lambda s: s.replace(" - - [", "|"))
df_full = df_full.map(lambda s: s.replace('] "', '|'))
df_full = df_full.map(lambda s: s.replace('" ', '|'))
df_full = df_full.map(lambda s: s.replace('alyssa.p', ''))
df_full = df_full.map(lambda line: line.split('|'))

#Cabeçario
file_base = df_full.toDF(['Host','Datahora','Requisicao','Codigo_http_bytes'])

#Func de split column
split_col = f.split(f.col('Codigo_http_bytes'), ' ')

#Criando Df
df1 = file_base.withColumn('Codigo_http', split_col.getItem(0))\
    .withColumn('Total_bytes', split_col.getItem(1))\
        .select(
            f.col("Host"),
            f.col("Datahora"),
            f.col("Requisicao"),
            f.col("Codigo_http"),
            f.col("Total_bytes")
            )\
                .withColumn('Total_bytes', f.when(f.col('Total_bytes')=='-','0')\
                    .when(f.col('Total_bytes')=='','0')\
                        .when(f.col('Total_bytes')==' ','0')\
                            .when(f.col('Total_bytes').isNull(),'0')\
                                .otherwise(f.col('Total_bytes')))

#Número de hosts únicos.
df_host_unicos = df1.select(f.col("Host")).distinct().count()

#lista unicas de hosts
df_list_unic_host = df1.select(f.col("Host")).distinct()
#Gravando o arquivo no hdfs
saida1 = file_name_out+"-df_list_unic_host"
df_list_unic_host.coalesce(1).write.format("json").mode("overwrite").save(saida1)

#Total de erros 404.
df_error_404 = df1.select(f.col("Codigo_http"))\
    .filter(f.col("Codigo_http")=="404")\
        .groupBy(f.col("Codigo_http"))\
            .agg(f.count(f.col("Codigo_http")).alias("qtd_error")
)
#Gravando o arquivo no hdfs
saida1 = file_name_out+"-df_error_404"
df_error_404.coalesce(1).write.format("json").mode("overwrite").save(saida1)

#Os 5 URLs que mais causaram erro 404.
df_url_erro = df1.filter(f.col("Codigo_http")=="404")\
    .select(f.col("Host"))\
        .groupBy(f.col("Host"))\
            .agg(f.count(f.col("Host")).alias("qtd_host"))\
                .orderBy(f.col("qtd_host").desc())\
                    .limit(5)
#Gravando o arquivo no hdfs
saida1 = file_name_out+"-df_url_erro"
df_url_erro.coalesce(1).write.format("json").mode("overwrite").save(saida1)

#Quantidade de erros 404 por dia.
df_trunc_date = df1.filter(f.col("Codigo_http")=="404")\
    .withColumn("data", f.unix_timestamp(f.col("Datahora"),"dd/MMM/yyyy").cast("double").cast("timestamp"))\
        .select(f.col("data"),f.col("Codigo_http"))\
            .groupBy(f.col("data"))\
                .agg(f.count(f.col("Codigo_http")).alias("qtd_erro"))\
                    .orderBy(f.col('data').asc())
#Gravando o arquivo no hdfs
saida1 = file_name_out+"-df_trunc_date"
df_trunc_date.coalesce(1).write.format("json").mode("overwrite").save(saida1)

#O total de bytes retornados.
df_total_bytes = df1.select(f.col("Total_bytes"))\
    .agg(f.sum(f.col("Total_bytes")).alias("Qtd_bytes"))
#Gravando o arquivo no hdfs
saida1 = file_name_out+"-df_total_bytes"
df_total_bytes.coalesce(1).write.format("json").mode("overwrite").save(saida1)
