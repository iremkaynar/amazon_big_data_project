import pyspark
from pyspark.sql import SparkSession

# Spark oturumu başlat
spark = (
	SparkSession.builder.appName("VerifyData")
	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
	.getOrCreate()
)

# Delta tablosunu oku
df = spark.read.format("delta").load("/tmp/delta/amazon_reviews/silver_reviews")

# Toplam satır sayısını yazdır
print(f"✅ Toplam işlenen satır: {df.count()}")

# İlk 10 satırı göster
print("\n📊 İlk 10 satır:")
df.show(10)

# Sütun adları ve tipler
print("\n🔍 Veri Schema:")
df.printSchema()

# Kategorilere göre özet
print("\n📈 Kategorilere göre dağılım:")
df.groupBy("kategori").count().show()

spark.stop()
print("\n✅ Doğrulama tamamlandı!")
