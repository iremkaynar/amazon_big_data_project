from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark oturumunu başlatıyoruz
spark = SparkSession.builder \
    .appName("AmazonReviewsStreaming") \
    .getOrCreate()

# 1. ŞEMA (SCHEMA) TANIMLAMA
schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("kullanici_ID", StringType(), True),
    StructField("olay_tipi", StringType(), True),
    StructField("ilgili_ID", StringType(), True),
    StructField("kategori", StringType(), True)
])

# 2. KAFKA'DAN SÜREKLİ OKUMA
# Docker container'ında kafka service'e docker network adıyla bağlanıyoruz
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "amazon_reviews_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. BRONZE KATMANI
# Ham mesajı olduğu gibi Parquet'e yazıyoruz. Bu katman denetim ve geri izleme için kullanılır.
bronze_df = kafka_df.select(
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("raw_json")
)

bronze_query = bronze_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/amazon_reviews/bronze") \
    .option("path", "/tmp/parquet/bronze_reviews") \
    .start()

# 4. SILVER KATMANI
# Gelen JSON veriyi schema ile parse edip temizliyoruz.
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

cleaned_df = parsed_df.dropna().dropDuplicates(["kullanici_ID", "ilgili_ID", "timestamp"])

silver_query = cleaned_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/amazon_reviews/silver") \
    .option("path", "/tmp/parquet/silver_reviews") \
    .start()

# Bronze ve Silver akışını başlat ve çalışmaya bırak.
spark.streams.awaitAnyTermination()