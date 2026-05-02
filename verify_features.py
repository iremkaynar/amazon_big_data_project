from pyspark.sql import SparkSession


spark = (
    SparkSession.builder.appName("VerifyFeatures")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df = spark.read.format("delta").load("/tmp/delta/features/review_features")

print(f"Toplam feature satırı: {df.count()}")
print("\nSchema:")
df.printSchema()

print("\nİlk 10 satır:")
df.show(10, truncate=False)

print("\nFeature özetleri:")
df.select(
    "event_hour",
    "day_of_week",
    "is_weekend",
    "user_review_count",
    "product_review_count",
    "day_review_count",
    "hour_review_count",
).summary().show(truncate=False)

spark.stop()