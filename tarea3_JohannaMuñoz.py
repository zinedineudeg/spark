#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

#imprimimos el esquema
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


#creacion de archivo kafka_producer.py

import time
import json
import random
from kafka import KafkaProducer
def generate_sensor_data():
 return {
 "sensor_id": random.randint(1, 10),
 "temperature": round(random.uniform(20, 30), 2),
 "humidity": round(random.uniform(30, 70), 2),
 "timestamp": int(time.time())
 }
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
 value_serializer=lambda x: json.dumps(x).encode('utf-8'))
while True:
 sensor_data = generate_sensor_data()
 producer.send('sensor_data', value=sensor_data)
 print(f"Sent: {sensor_data}")
 time.sleep(1)

#Implementación del consumidor con Spark Streaming

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
import logging

# Configura el nivel de log a WARN para reducir los mensajes INFO
spark = SparkSession.builder \
 .appName("KafkaSparkStreaming") \
 .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de entrada
schema = StructType([
 StructField("sensor_id", IntegerType()),
 StructField("temperature", FloatType()),
 StructField("humidity", FloatType()),
 StructField("timestamp", TimestampType())
])

# Crear una sesión de Spark
spark = SparkSession.builder \
 .appName("SensorDataAnalysis") \
 .getOrCreate()

# Configurar el lector de streaming para leer desde Kafka
df = spark \
 .readStream \
 .format("kafka") \
 .option("kafka.bootstrap.servers", "localhost:9092") \
 .option("subscribe", "sensor_data") \
 .load()

# Parsear los datos JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calcular estadísticas por ventana de tiempo
windowed_stats = parsed_df \
 .groupBy(window(col("timestamp"), "1 minute"), "sensor_id") \
 .agg({"temperature": "avg", "humidity": "avg"})

# Escribir los resultados en la consola
query = windowed_stats \
 .writeStream \
 .outputMode("complete") \
 .format("console") \
 .start()
query.awaitTermination()
