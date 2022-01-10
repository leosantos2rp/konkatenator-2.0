# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkContext
import sys
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

def orcOrParquet(user, database, table, hdfsPath):
    # Define se a tabela é orc ou parquet
    orcInputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    parquetInputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"

    #Query para metadata da tabela
    df = spark.sql("describe formatted {0}.{1}".format(database, table))
    dfDict = df.toPandas().set_index('col_name')['data_type'].to_dict()

    # Condicional que verifica o formato da tabela e chama a funcao correspondente
    if dfDict["InputFormat"] == orcInputFormat:
        concatOrc(hdfsPath, user)
    elif dfDict["InputFormat"] == parquetInputFormat:
        concatParquet(hdfsPath, user)
    
def concatParquet(hdfsPath, user):
    # Usa a funcao coalesce() para concatenar os arquivos
    sc.setCheckpointDir('/user/{0}/sparkCheckpoint'.format(user))
    df = spark.read.parquet(hdfsPath).checkpoint()
    df.coalesce(1).write.mode("overwrite").parquet(hdfsPath)

def concatOrc(hdfsPath, user):
    # Usa a funcao coalesce() para concatenar os arquivos
    sc.setCheckpointDir('/user/{0}/sparkCheckpoint'.format(user))
    df = spark.read.orc(hdfsPath).checkpoint()
    df.coalesce(1).write.mode("overwrite").orc(hdfsPath)

# Pega os argumentos necessários para concatenação
args = sys.argv

user = args[1]
database = args[2]
full = bool(args[3])

# Query para coletar as tabelas do db
tablesDF = spark.sql("show tables in {}".format(database))
# Converte o df resultante em list com os nomes das tabelas
tablesList = tablesDF.toPandas()['tableName'].to_list()

# Loop para percorrer todas as tabelas do db
for table in tablesList:
    # Coleta as particoes de cada tabela
    partitionsDF = spark.sql("show partitions {0}.{1}".format(database, table))
    
    # Converte o df resultante em list com os nomes das particoes da tabela
    partitionsList = partitionsDF.toPandas()['partition'].to_list()

    # Caso deseje realizar full será rodado para todas as particoes, caso contrario apenas para as 3 ultimas
    if not full:
        partitionsList = partitionsList[-3:]
    
    # Para cada particao
    for partition in partitionsList:
        # Monta caminho HDFS
        hdfsPath = "/user/{0}/warehouse/{1}.db/{2}/{3}/".format(user, database, table, partition)
        try:
            # Chama a funcao que captura o file format da tabela
            orcOrParquet(user, database, table, hdfsPath)
        except Exception as e:
            print("Falha ao concatenar {0}".format(hdfsPath))
            print(e)