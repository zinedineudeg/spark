# Importamos librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
print("Inicializando la sesión de Spark.")
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Ajusta el número máximo de campos para la representación en cadena
spark.conf.set("spark.sql.debug.maxToStringFields", 100)

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/h2yr-zfb2.csv'
print(f"Leyendo el archivo CSV desde HDFS ubicado en la ruta: {file_path}.")

# Lee el archivo .csv
df = spark.read.format('csv') \
    .option('header', 'true') \
    .option('inferSchema', 'true') \
    .option('encoding', 'latin1') \
    .load(file_path)

# Imprimimos el esquema
print("\nEsquema del DataFrame:")
df.printSchema()

# Muestra las primeras filas del DataFrame
print("\nPrimeras filas del DataFrame:")
df.show(truncate=False)

# Estadísticas básicas
print("\nEstadísticas básicas del DataFrame:")
df.summary().show()


# Filtrar el DataFrame por el municipio "BOGOTÁ D.C."
print("Filtrando registros para el municipio de BOGOTÁ D.C.")
bogota_df = df.filter(F.col("municipio") == "BOGOTÁ D.C.")

# me cuenta la cantidad que hay por cada municipio
resultado = bogota_df.groupBy(
    "fecha_hecho",
    "cod_depto",
    "DEPARTAMENTO",
    "departamento",
    "COD_MUNI",
    "cod_muni",
    "municipio",
    "descripcion_conducta"
).agg(F.count("*").alias("cantidad"))
resultado.show(truncate=False)

# Agrupar por municipio y contar la cantidad de casos delictivos informaticos que hubieron
print("Obteniendo el municipio con la mayor cantidad de casos:")
municipio_mas_casos = df.groupBy("municipio") \
    .agg(F.count("*").alias("cantidad_casos")) \
    .orderBy(F.col("cantidad").desc()) \
    .limit(1)  
municipio_mas_casos.show(truncate=False)

# Filtrar los datos excluyendo los registros del municipio de "BOGOTA D.C."
print("Obteniendo todos los datos excepto los del municipio BOGOTA D.C.")
datos_sin_bogota = df.filter(F.col("municipio") != "BOGOTA D.C.")
datos_sin_bogota.show(truncate=False)