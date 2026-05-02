"""
NLP Sentiment Analysis Pipeline
================================
Bu script Silver katmanındaki yorum metinlerinden (review_body)
VADER Sentiment Analyzer kullanarak duygu skorları çıkarır.

Çıktı:
- Feature Store'a (Delta Lake) sentiment score, sentiment_label ile birlikte yazılır
- Arkadaşınız bu feature'ları ML modeline input olarak kullanabilir

Aşamalar:
1. Silver Parquet'i oku (temizlenmiş veriler)
2. VADER sentiment analizi yap
3. Feature engineering feature'larıyla birleştir
4. Sentiment feature'ları ekle
5. Delta Lake'e kaydet (Feature Store)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    dayofweek,
    from_unixtime,
    hour,
    to_date,
    when,
    udf,
    lit,
)
from pyspark.sql.types import DoubleType, StringType
from textblob import TextBlob
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Spark oturumunu başlat - Delta Lake desteği ile
spark = (
    SparkSession.builder.appName("NLPSentimentPipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ========================
# 1. VERILER OKUNUR
# ========================
logger.info("Silver katmanından veriler okunuyor...")
silver_path = "/tmp/parquet/silver_reviews"
silver_df = spark.read.parquet(silver_path)

logger.info(f"Silver veri: {silver_df.count()} satır")

# ========================
# 2. SENTIMENT ANALIZI FONKSİYONU
# ========================
# TextBlob'u kullanalım - basit, hızlı, Türkçe ve İngilizce destekler
def get_sentiment_score(text):
    """
    TextBlob Sentiment Polarity Score:
    - (-1, 0): Negative (olumsuz)
    - (0, 0.33): Weakly Positive (zayıf olumlu)
    - (0.33, 0.67): Neutral (tarafsız)
    - (0.67, 1): Positive (olumlu)
    """
    if text is None or text == "":
        return 0.0
    try:
        blob = TextBlob(str(text))
        polarity = blob.sentiment.polarity  # (-1, 1) aralığında
        # Normalize et: (-1, 1) -> (0, 1)
        normalized_score = (polarity + 1) / 2
        return float(normalized_score)
    except Exception as e:
        logger.warning(f"Sentiment analizi hatası: {e}")
        return 0.5  # Varsayılan (tarafsız)


def get_sentiment_label(score):
    """Sentiment score'u kategoriye çevir"""
    if score < 0.33:
        return "Negative"  # Olumsuz
    elif score < 0.67:
        return "Neutral"  # Tarafsız
    else:
        return "Positive"  # Olumlu


# UDF'leri Spark'a kaydedelim
sentiment_score_udf = udf(get_sentiment_score, DoubleType())
sentiment_label_udf = udf(get_sentiment_label, StringType())

# ========================
# 3. SENTIMENT FEATURE'LARI HESAPLA
# ========================
logger.info("Sentiment analizi yapılıyor...")
df_with_sentiment = silver_df.withColumn(
    "sentiment_score", sentiment_score_udf(col("review_body"))
).withColumn(
    "sentiment_label", sentiment_label_udf(col("sentiment_score"))
)

# ========================
# 4. TARİH VE ZAMAN FEATURESİ EKLE
# ========================
# (Feature Engineering'den alınan benzer logic)
df_with_temporal = (
    df_with_sentiment
    .withColumn("event_date", to_date(from_unixtime(col("timestamp"))))
    .withColumn("event_hour", hour(from_unixtime(col("timestamp"))))
    .withColumn("day_of_week", dayofweek(from_unixtime(col("timestamp"))))
    .withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), 1).otherwise(0),
    )
)

# ========================
# 5. İSTATİSTİKLER HESAPLA
# ========================
# Kullanıcı ve ürün yoğunluğu
user_counts = (
    df_with_temporal
    .groupBy("kullanici_ID")
    .agg(count("*").alias("user_review_count"))
)
product_counts = (
    df_with_temporal
    .groupBy("ilgili_ID")
    .agg(count("*").alias("product_review_count"))
)
day_counts = (
    df_with_temporal
    .groupBy("event_date")
    .agg(count("*").alias("day_review_count"))
)
hour_counts = (
    df_with_temporal
    .groupBy("event_hour")
    .agg(count("*").alias("hour_review_count"))
)

# ========================
# 6. TÜM FEATURESİ BİRLEŞTİR
# ========================
logger.info("Tüm features birleştiriliyor...")
feature_df = (
    df_with_temporal
    .join(user_counts, on="kullanici_ID", how="left")
    .join(product_counts, on="ilgili_ID", how="left")
    .join(day_counts, on="event_date", how="left")
    .join(hour_counts, on="event_hour", how="left")
)

# Final seçim: orijinal + duygu + temporal + istatistikler
feature_df_final = feature_df.select(
    # Orijinal alanlar
    "timestamp",
    "kullanici_ID",
    "ilgili_ID",
    "kategori",
    "olay_tipi",
    "star_rating",
    "review_body",
    # Duygu (NLP) features
    "sentiment_score",
    "sentiment_label",
    # Temporal features
    "event_date",
    "event_hour",
    "day_of_week",
    "is_weekend",
    # İstatistiksel features
    "user_review_count",
    "product_review_count",
    "day_review_count",
    "hour_review_count",
)

# ========================
# 7. FEATURE STORE'A YAZDIR (DELTA LAKE)
# ========================
logger.info("Feature Store'a yazılıyor...")
feature_path = "/tmp/delta/features/review_features_with_sentiment"

feature_df_final.write.format("delta").mode("overwrite").save(feature_path)

logger.info(f"✅ Feature tablosu yazıldı: {feature_path}")
logger.info(f"   Toplam satır: {feature_df_final.count()}")

# ========================
# 8. ÖZET VE ISTATISTIKLER
# ========================
logger.info("\n=== SENTIMENT ANALIZI ÖZETI ===")
feature_df_final.groupBy("sentiment_label").count().show()

logger.info("\n=== SENTIMENT SCORE ISTATISTIKLERI ===")
feature_df_final.agg(
    {"sentiment_score": ["min", "max", "avg"]}
).show()

logger.info("\n=== SCHEMA ===")
feature_df_final.printSchema()

logger.info("\n=== İLK 5 KAYIT ===")
feature_df_final.show(5, truncate=False)

logger.info("\n🎉 NLP Sentiment Pipeline tamamlandı!")
logger.info("Çıktı: Feature Store'da 'review_features_with_sentiment'")
logger.info("ML modeliniz bu feature'ları kullanabilir:")
logger.info("  - sentiment_score: 0-1 arası duygu puanı")
logger.info("  - sentiment_label: Negatif/Tarafsız/Pozitif kategorisi")
logger.info("  - Diğer tüm feature'lar (temporal, istatistiksel)")

spark.stop()
