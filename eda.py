import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, date_format, from_unixtime, to_date, hour, dayofweek
from datetime import datetime

# Spark oturumu
spark = SparkSession.builder.appName("EDA").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "="*80)
print("KEŞIFSEL VERİ ANALİZİ (EDA) - Silver Katmanı")
print("="*80 + "\n")

# Silver tablosunu oku
df = spark.read.parquet("/tmp/parquet/silver_reviews")

# ============================================================================
# 1. TEMEL İSTATİSTİKLER
# ============================================================================
print("📊 1. TEMEL İSTATİSTİKLER")
print("-" * 80)

toplam_satir = df.count()
print(f"   • Toplam satır sayısı: {toplam_satir:,}")

# Şema
print(f"\n   • Veri şeması:")
df.printSchema()

# ============================================================================
# 2. BENZERSİZ DEĞER SAYILARI
# ============================================================================
print("\n📈 2. BENZERSİZ DEĞER SAYILARI")
print("-" * 80)

unique_stats = df.select(
    countDistinct("kullanici_ID").alias("Benzersiz Kullanıcı"),
    countDistinct("ilgili_ID").alias("Benzersiz Ürün"),
    countDistinct("kategori").alias("Benzersiz Kategori"),
    countDistinct("olay_tipi").alias("Benzersiz Olay")
).collect()[0]

print(f"   • Benzersiz Kullanıcı: {unique_stats[0]:,}")
print(f"   • Benzersiz Ürün: {unique_stats[1]:,}")
print(f"   • Benzersiz Kategori: {unique_stats[2]:,}")
print(f"   • Benzersiz Olay Tipi: {unique_stats[3]:,}")

# ============================================================================
# 3. EKSİK DEĞER ANALİZİ
# ============================================================================
print("\n🔍 3. EKSİK DEĞER ANALİZİ")
print("-" * 80)

null_counts = df.select([count(col(c)).cast("long").alias(c) for c in df.columns]).collect()[0]
print(f"   • Toplam satır: {toplam_satir:,}")
print(f"   • Eksik (null) değer sayısı:")
for col_name in df.columns:
    missing = toplam_satir - null_counts[col_name]
    if missing > 0:
        print(f"     - {col_name}: {missing:,} ({missing/toplam_satir*100:.2f}%)")
    else:
        print(f"     - {col_name}: 0 (Tamam ✓)")

# ============================================================================
# 4. KATEGORİK DEĞİŞKEN DAĞILIMI
# ============================================================================
print("\n📋 4. KATEGORİK DEĞİŞKEN DAĞILIMI")
print("-" * 80)

print("\n   Kategori dağılımı:")
df.groupBy("kategori").agg(count("*").alias("Sayı")).show(truncate=False)

print("\n   Olay tipi dağılımı:")
df.groupBy("olay_tipi").agg(count("*").alias("Sayı")).show(truncate=False)

# ============================================================================
# 5. ZAMAN SERİSİ ANALİZİ
# ============================================================================
print("\n⏰ 5. ZAMAN SERİSİ ANALİZİ")
print("-" * 80)

# Unix timestamp'i datetime'a çevir
df_with_date = df.withColumn("date", to_date(from_unixtime("timestamp")))
df_with_date = df_with_date.withColumn("hour", hour(from_unixtime("timestamp")))
df_with_date = df_with_date.withColumn("day_of_week", dayofweek(from_unixtime("timestamp")))

print("\n   Günlük yorum sayısı:")
daily_stats = df_with_date.groupBy("date").agg(count("*").alias("Yorum Sayısı")).orderBy("date").collect()
if daily_stats:
    for row in daily_stats[-10:]:  # Son 10 gün göster
        print(f"     - {row[0]}: {row[1]:,} yorum")
else:
    print("     - Veri yok")

print("\n   Saatlik dağılım (ilk 10 saat):")
hourly_stats = df_with_date.groupBy("hour").agg(count("*").alias("Yorum Sayısı")).orderBy("hour").collect()
for row in hourly_stats[:10]:
    print(f"     - Saat {int(row[0]):02d}:00: {row[1]:,} yorum")

# ============================================================================
# 6. SAYISAL DEĞİŞKEN ANALİZİ (timestamp)
# ============================================================================
print("\n🔢 6. SAYISAL DEĞİŞKEN ANALİZİ (timestamp)")
print("-" * 80)

from pyspark.sql.functions import min as spark_min, max as spark_max, avg as spark_avg

stats = df.select(
    spark_min("timestamp").alias("min_ts"),
    spark_max("timestamp").alias("max_ts"),
    spark_avg("timestamp").alias("avg_ts")
).collect()[0]

min_dt = datetime.fromtimestamp(stats[0])
max_dt = datetime.fromtimestamp(stats[1])

print(f"   • En eski veri: {min_dt}")
print(f"   • En yeni veri: {max_dt}")
print(f"   • Zaman aralığı: {(max_dt - min_dt).days} gün")

# ============================================================================
# 7. ÖZETİ İLK KAYITLAR
# ============================================================================
print("\n📝 7. ÖRNEK KAYITLAR (İlk 5)")
print("-" * 80)
df.show(5, truncate=False)

print("\n" + "="*80)
print("✅ EDA tamamlandı!")
print("="*80 + "\n")

spark.stop()
