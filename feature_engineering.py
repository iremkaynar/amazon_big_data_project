from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    dayofweek,
    from_unixtime,
    hour,
    to_date,
    when,
)


spark = (
    SparkSession.builder.appName("FeatureEngineering")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


silver_path = "/tmp/parquet/silver_reviews"
feature_path = "/tmp/delta/features/review_features"


# Silver katmanı okunur ve ML için anlamlı öznitelikler üretilir.
df = spark.read.parquet(silver_path)

base_df = df.withColumn("event_date", to_date(from_unixtime(col("timestamp"))))
base_df = base_df.withColumn("event_hour", hour(from_unixtime(col("timestamp"))))
base_df = base_df.withColumn("day_of_week", dayofweek(from_unixtime(col("timestamp"))))
base_df = base_df.withColumn(
    "is_weekend",
    when(col("day_of_week").isin([1, 7]), 1).otherwise(0),
)

# Kullanıcı ve ürün yoğunluğu, etkileşim gücü hakkında sinyal verir.
user_counts = df.groupBy("kullanici_ID").agg(count("*").alias("user_review_count"))
product_counts = df.groupBy("ilgili_ID").agg(count("*").alias("product_review_count"))
day_counts = base_df.groupBy("event_date").agg(count("*").alias("day_review_count"))
hour_counts = base_df.groupBy("event_hour").agg(count("*").alias("hour_review_count"))

feature_df = (
    base_df.join(user_counts, on="kullanici_ID", how="left")
    .join(product_counts, on="ilgili_ID", how="left")
    .join(day_counts, on="event_date", how="left")
    .join(hour_counts, on="event_hour", how="left")
)


feature_df = feature_df.select(
    "timestamp",
    "kullanici_ID",
    "ilgili_ID",
    "kategori",
    "olay_tipi",
    "event_date",
    "event_hour",
    "day_of_week",
    "is_weekend",
    "user_review_count",
    "product_review_count",
    "day_review_count",
    "hour_review_count",
)


print("Oluşturulan özellikler:")
feature_df.printSchema()
print("İlk 5 kayıt:")
feature_df.show(5, truncate=False)


feature_df.write.format("delta").mode("overwrite").save(feature_path)

print(f"Feature tablosu yazıldı: {feature_path}")

spark.stop()