from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("InversionesStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("clase", StringType()),
    StructField("moneda", StringType()),
    StructField("valor_mercado", DoubleType()),
    StructField("timestamp", TimestampType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "inversiones") \
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

stats = parsed.groupBy(window(col("timestamp"), "1 minute"), "moneda") \
    .agg(F.count("*").alias("eventos"),
         F.round(F.avg("valor_mercado") / 1e9, 2).alias("promedio_miles_millones"))

query = stats.writeStream.outputMode("complete").format("console").option("truncate", "false").start()
query.awaitTermination()
