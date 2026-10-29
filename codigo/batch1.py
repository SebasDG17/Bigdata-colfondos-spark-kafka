from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('Tarea4Batch').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

ruta = 'hdfs://localhost:9000/Tarea4/dataset.csv'
df = spark.read.format('csv').option('header','true').option('inferSchema','true').load(ruta)

print(df.count(), len(df.columns))

cols = ['nombre_de_entidad', 'descripcion_clase_inversion',
        'descripcion_codigo_moneda', 'valor_de_compra_en_pesos',
        'valor_mercado_o_pres_pesos', 'tasa_facial_titulo']
df.select(cols).printSchema()
df.select(cols).show(10, truncate=False)
