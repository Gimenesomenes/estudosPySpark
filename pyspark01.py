from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
os.environ['JAVA_HOME'] = r'C:\Program Files\Amazon Corretto\jdk11.0.27_6'

#Criar sessão pyspark

spark = (
    SparkSession.builder
    .master('local')
    .appName('pyspark01')
    .getOrCreate()
)

df = spark.read.csv('wc2018-players.csv', header=True, inferSchema=True)

#print(df.show())

# Verificando tipos de colunas

print(df.printSchema())

# Data de nascimento como string -> mudar para inteiro

# Verificação de valores nulos

#print(df.toPandas().isna().sum())   # para DataFrames pequenos


# Renomeando colunas

df = df.withColumnRenamed('Pos.','Posicao').withColumnRenamed('#', 'Numero').withColumnRenamed('FIFA Popular Name', 'Nome_FIFA')\
.withColumnRenamed('Birth Date', 'Nascimento').withColumnRenamed('Shirt Name', 'Nome Camiseta').withColumnRenamed('Club', 'Time')\
.withColumnRenamed('Height', 'Altura').withColumnRenamed('Weight', 'Peso').withColumnRenamed('Team', 'Selecao')

df.show(5)

# No PySpark precisamos fazer assim para verificar valores nulos:

for coluna in df.columns:
    print(coluna, df.filter(df[coluna].isNull()).count())

# Selecionar colunas:

#df.select('Seleção', 'Nome_FIFA').show(5)   # Não é case sensitive

#df.select(col('Seleção'), col('Altura')).show(5)

#df.select(df['Seleção'], df['Altura']).show(5)

# Selecionando colunas com ALIAS (o que quer dizer é como você dar uma apelido para a coluna):

#df.select(df['Seleção'].alias('Time')).show(5)

# Filtrar 

#df.filter('Selecao = "Brazil"').show(5)

# colunas com espaço

#df.filter(col("Nome Camiseta") == "FRED").show()


# Filtrar com duas condições 'AND'

df.filter((col('Selecao') == "Argentina") & (col('Altura') > 180)).show()

# Criar colunas novas usando lit

df.withColumn('IMC', round(col('Peso') / ((col('Altura') / 100) ** 2),2)).show(5) # calculando IMC dos jogadores IMC = Peso(kg)/Altura(metros)²


# Criar colunas novas usando (substring)

df.withColumn('Ano', substring('Nascimento', -4, 4)).show()
df = df.withColumn('Ano', substring('Nascimento', -4, 4))

df.show()

# DESAFIO: Coluna nascimento
# Colocar como DataType
# Dica
# O formato precisa ser: YYYY.MM.DD

df.withColumn('Nascimento', to_date('Nascimento', "dd.MM.yyyy")).show()
df = df.withColumn('Nascimento', to_date('Nascimento', "dd.MM.yyyy"))

df.show()

df.printSchema()