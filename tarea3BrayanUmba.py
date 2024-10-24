# Importamos librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
print("Inicializando la sesión de Spark.")
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Ajusta el número máximo de campos para la representación en cadena
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

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

# Limpiar y convertir la columna 'valor_asignado' a tipo numérico (double) usando cast directo
print("Limpiando la columna 'valor_asignado' para eliminar espacios y convirtiéndola a tipo numérico.")
df = df.withColumn("Valor_Asignado", F.regexp_replace("valor_asignado", " ", "").cast("double"))

# Consulta: Estadísticas básicas por departamento
print("\nEstadísticas básicas por departamento:")
dept_stats = df.groupBy("Departamento").agg(
    F.count("*").alias("Total_Subsidios"),
    F.sum("Hogares").alias("Total_Hogares"),
    F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total_Asignado"),
    F.round(F.avg("Valor_Asignado"), 2).alias("Valor_Promedio")
)
dept_stats.show(truncate=False)

# Consulta: Top 10 municipios con mayor valor total asignado
print("\nTop 10 municipios con mayor valor total asignado:")
top_municipios = df.groupBy("Municipio", "Departamento") \
    .agg(F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total")) \
    .orderBy(F.col("Valor_Total").desc())
top_municipios.show(10, truncate=False)

# Consulta: Distribución de subsidios por programa
print("\nDistribución de subsidios por programa:")
programa_stats = df.groupBy("Programa").agg(
    F.count("*").alias("Cantidad_Subsidios"),
    F.sum("Hogares").alias("Total_Hogares"),
    F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total")
).orderBy(F.col("Valor_Total").desc())
programa_stats.show(truncate=False)

# Consulta: Tendencia por año
print("\nTendencia por año:")
tendencia_anual = df.groupBy("a_o_de_asignaci_n").agg(
    F.count("*").alias("Cantidad_Subsidios"),
    F.sum("Hogares").alias("Total_Hogares"),
    F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total")
).orderBy("a_o_de_asignaci_n")
tendencia_anual.show(truncate=False)

# Consulta: Estado de las postulaciones
print("\nEstado de las postulaciones:")
estado_stats = df.groupBy("estado_de_postulaci_n").agg(
    F.count("*").alias("Cantidad"),
    F.sum("Hogares").alias("Total_Hogares"),
    F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total")
)
estado_stats.show(truncate=False)
vboxuser@bigdata:~$ ^C
vboxuser@bigdata:~$ cat  tarea3.py
# Importamos librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
print("Inicializando la sesión de Spark.")
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Ajusta el número máximo de campos para la representación en cadena
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

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

# Limpiar y convertir la columna 'valor_asignado' a tipo numérico (double) usando cast directo
print("Limpiando la columna 'valor_asignado' para eliminar espacios y convirtiéndola a tipo numérico.")
df = df.withColumn("Valor_Asignado", F.regexp_replace("valor_asignado", " ", "").cast("double"))

# Consulta: Estadísticas básicas por departamento
print("\nEstadísticas básicas por departamento:")
dept_stats = df.groupBy("Departamento").agg(
    F.count("*").alias("Total_Subsidios"),
    F.sum("Hogares").alias("Total_Hogares"),
    F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total_Asignado"),
    F.round(F.avg("Valor_Asignado"), 2).alias("Valor_Promedio")
)
dept_stats.show(truncate=False)

# Consulta: Top 10 municipios con mayor valor total asignado
print("\nTop 10 municipios con mayor valor total asignado:")
top_municipios = df.groupBy("Municipio", "Departamento") \
    .agg(F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total")) \
    .orderBy(F.col("Valor_Total").desc())
top_municipios.show(10, truncate=False)

# Consulta: Distribución de subsidios por programa
print("\nDistribución de subsidios por programa:")
programa_stats = df.groupBy("Programa").agg(
    F.count("*").alias("Cantidad_Subsidios"),
    F.sum("Hogares").alias("Total_Hogares"),
    F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total")
).orderBy(F.col("Valor_Total").desc())
programa_stats.show(truncate=False)

# Consulta: Tendencia por año
print("\nTendencia por año:")
tendencia_anual = df.groupBy("a_o_de_asignaci_n").agg(
    F.count("*").alias("Cantidad_Subsidios"),
    F.sum("Hogares").alias("Total_Hogares"),
    F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total")
).orderBy("a_o_de_asignaci_n")
tendencia_anual.show(truncate=False)

# Consulta: Estado de las postulaciones
print("\nEstado de las postulaciones:")
estado_stats = df.groupBy("estado_de_postulaci_n").agg(
    F.count("*").alias("Cantidad"),
    F.sum("Hogares").alias("Total_Hogares"),
    F.round(F.sum("Valor_Asignado"), 2).alias("Valor_Total")
)
estado_stats.show(truncate=False)
