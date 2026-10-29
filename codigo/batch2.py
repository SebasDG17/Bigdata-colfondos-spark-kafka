from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('Tarea4Batch2').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ruta = 'hdfs://localhost:9000/Tarea4/dataset.csv'
df = spark.read.format('csv').option('header','true').option('inferSchema','true').load(ruta)

# LIMPIEZA
print("Filas iniciales:", df.count())
df = df.dropDuplicates()
df = df.select('nombre_de_entidad', 'descripcion_clase_inversion',
               'descripcion_codigo_moneda', 'valor_de_compra_en_pesos',
               'valor_mercado_o_pres_pesos')
df = df.dropna()
df = df.filter(F.col('valor_de_compra_en_pesos') > 0)
print("Filas despues de limpieza:", df.count())

# TRANSFORMACION
df = df.withColumn('variacion_pesos',
                   F.col('valor_mercado_o_pres_pesos') - F.col('valor_de_compra_en_pesos'))

# EDA
print("Estadisticas basicas")
df.describe('valor_de_compra_en_pesos', 'valor_mercado_o_pres_pesos', 'variacion_pesos').show()

print("Valor de mercado por clase de inversion (miles de millones COP)")
por_clase = df.groupBy('descripcion_clase_inversion') \
    .agg(F.count('*').alias('titulos'),
         F.round(F.sum('valor_mercado_o_pres_pesos') / 1e9, 1).alias('mercado')) \
    .orderBy(F.desc('mercado'))
por_clase.show(10, truncate=False)

print("Titulos por moneda")
df.groupBy('descripcion_codigo_moneda').count().orderBy(F.desc('count')).show(10, truncate=False)

print("Top 5 entidades por valor de mercado (miles de millones COP)")
df.groupBy('nombre_de_entidad') \
    .agg(F.round(F.sum('valor_mercado_o_pres_pesos') / 1e9, 1).alias('mercado')) \
    .orderBy(F.desc('mercado')).show(5, truncate=False)

# VISUALIZACION EN CONSOLA
print("\nGrafica: valor de mercado por clase de inversion")
filas = por_clase.limit(8).collect()
maximo = filas[0]['mercado']
for r in filas:
    barra = '#' * int(r['mercado'] / maximo * 40)
    print(f"{r['descripcion_clase_inversion'][:35]:35} {barra} {r['mercado']}")
