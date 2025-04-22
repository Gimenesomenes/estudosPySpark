from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
os.environ['JAVA_HOME'] = r'C:\Program Files (x86)\Amazon Corretto\jdk11.0.27_6'

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

