# Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/Reporte_Delito_Violencia_Intrafamiliar_Polic_a_Nacional.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Estadisticas básicas
df.summary().show()

# Consulta: Filtrar por valor y seleccionar columnas
print("Dias con valor mayor a 5000\n")
dias = df.filter(F.col('VALOR') > 5000).select('VALOR','VIGENCIADESDE','VIGENCIAHASTA')
dias.show()

# Ordenar filas por los valores en la columna "VALOR" en orden descendente
print("Valores ordenados de mayor a menor\n")
sorted_df = df.sort(F.col("VALOR").desc())
sorted_df.show()

#Kafka_producer.oy

import time
import json
import pandas as pd
from kafka import KafkaProducer
from hdfs import InsecureClient

# Conectar con HDFS
client = InsecureClient('http://localhost:9870', user='hadoop')

# Leer el archivo CSV desde HDFS
with client.read('/Tarea3/Reporte_Delito_Violencia_Intrafamiliar_Polic_a_Nacional.csv', encoding='utf-8') as reader:
    df = pd.read_csv(reader)

# Configurar el productor de Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Enviar datos al tópico de Kafka
for index, row in df.iterrows():
    data = row.to_dict()
    producer.send('domestic_violence_data', value=data)
    print(f"Sent: {data}")
    time.sleep(1)  # Simular la llegada de datos en tiempo real

    #Spark_streaming_consumer.py

    from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Configura el nivel de log a WARN para reducir los mensajes INFO
spark = SparkSession.builder \
    .appName("BatchProcessing") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de entrada basado en el dataset de Kaggle
schema = StructType([
    StructField("Año", IntegerType()),
    StructField("Mes", StringType()),
    StructField("Código Departamento", IntegerType()),
    StructField("Departamento", StringType()),
    StructField("Código Municipio", IntegerType()),
    StructField("Municipio", StringType()),
    StructField("Edad", IntegerType()),
    StructField("Sexo", StringType()),
    StructField("Estado Civil", StringType()),
    StructField("Nivel Educativo", StringType()),
    StructField("Fecha Hecho", TimestampType())
])

# Leer el archivo desde HDFS
file_path = 'hdfs://localhost:9000/Tarea3/Reporte_Delito_Violencia_Intrafamiliar_Polic_a_Nacional.csv'

# Cargar el archivo CSV en un DataFrame
df = spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .schema(schema) \
    .load(file_path)

# Calcular estadísticas por ventana de tiempo
windowed_stats = df \
    .groupBy(window(col("Fecha Hecho"), "1 minute"), "Departamento") \
    .agg({"Edad": "avg"}) \
    .persist()  # Asegura que el DataFrame se persista en memoria y disco

# Escribir los resultados a la consola
windowed_stats.show()

#Link de la información:
# https://www.kaggle.com/datasets/oscardavidperilla/domestic-violence-in-colombia/data