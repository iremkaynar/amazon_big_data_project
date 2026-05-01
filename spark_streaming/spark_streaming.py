from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark oturumunu başlatıyoruz
spark = SparkSession.builder \
    .appName("AmazonReviewsStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. ŞEMA (SCHEMA) TANIMLAMA
schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("kullanici_ID", StringType(), True),
    StructField("olay_tipi", StringType(), True),
    StructField("ilgili_ID", StringType(), True),
    StructField("kategori", StringType(), True)
])

# 2. KAFKA'DAN SÜREKLİ OKUMA (AUTO LOADER MANTIĞI)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "amazon_reviews_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Gelen karmaşık veriyi JSON şemamızla okunabilir hale getiriyoruz
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Proje yönergesine göre veri temizleme (Null değerleri ve duplike kayıtları atıyoruz)
cleaned_df = parsed_df.dropna().dropDuplicates(["kullanici_ID", "ilgili_ID", "timestamp"])

# 3. CHECKPOINT İLE DELTA LAKE'E YAZMA (SILVER LAYER)
query = cleaned_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/amazon_reviews") \
    .start("/tmp/delta/silver_reviews")

query.awaitTermination()