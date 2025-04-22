from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Criar sessão pyspark

spark = (
    SparkSession.builder
    .master('local')
    .appName('pyspark01')
    .getOrCreate()
)

df = spark.read.csv('wc2018-players.csv', header=True, inferSchema=True
                    )

print(df.show())
