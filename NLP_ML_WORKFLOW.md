# 📊 Amazon Reviews - Big Data Pipeline
## NLP & ML Workflow Dokümanı

### 🎯 Proje Hedefi
Amazon ürün yorumlarından **duygu analizi (Sentiment Analysis)** yapıp, kullanıcı davranışlarını **kümeleme (Clustering)** yoluyla segmentlendirmek.

---

## 📈 Veri Akış Mimarisi (Medallion Architecture)

```
CSV Data
   ↓
Producer (Kafka'ya stream et)
   ↓
Kafka Broker (amazon_reviews_topic)
   ↓
Spark Streaming
   ├─→ Bronze Layer (Ham veriler, /tmp/parquet/bronze_reviews)
   ├─→ Silver Layer (Temizlenmiş veriler, /tmp/parquet/silver_reviews)
   └─→ Gold Layer (Agregasyon, /tmp/parquet/gold_reviews)
   ↓
NLP Sentiment Pipeline (bu aşama)
   ├─→ TextBlob ile Sentiment Analizi
   ├─→ Temporal Features
   ├─→ Statistical Features
   └─→ Feature Store (Delta Lake, /tmp/delta/features/review_features_with_sentiment)
   ↓
ML Model (Arkadaşınızın yapacağı)
   ├─→ Kümeleme (K-Means, DBSCAN, vs.)
   └─→ Sınıflandırma (Sentiment → Label)
```

---

## 🔄 Detaylı Aşamalar

### Aşama 1: Producer (Kafka'ya Veri Gönder)
**Dosya**: `producer/producer.py`
**Yapıdığı şey**: CSV satırlarını JSON olarak Kafka topic'ine gönderir.

```bash
python producer/producer.py --rate 20
```

**Kafka Mesaj Format**:
```json
{
  "timestamp": 1746259200.0,
  "kullanici_ID": "41409413",
  "olay_tipi": "review_posted",
  "ilgili_ID": "B00428R89M",
  "kategori": "Electronics",
  "star_rating": 5.0,
  "review_body": "As described. Excellent product!"
}
```

---

### Aşama 2: Spark Streaming (Bronze/Silver/Gold)
**Dosya**: `spark_streaming/spark_streaming.py`
**Yapıdığı şey**: 
- **Bronze**: Ham Kafka mesajlarını Parquet'e yazar (denetim amaçlı)
- **Silver**: JSON'ı parse eder, null'ları kaldırır, duplicate'leri çıkarır
- **Gold**: Kategori bazlı agregasyonlar (review sayısı, unique kullanıcılar, vb.)

```bash
docker-compose up -d
python -c "from pyspark.sql import SparkSession; ..."
```

**Çıktı Konumları**:
- Bronze: `/tmp/parquet/bronze_reviews` (520KB, ~200 dosya)
- Silver: `/tmp/parquet/silver_reviews` (50MB, 10,095 satır)
- Gold: `/tmp/parquet/gold_reviews` (agregasyon)

---

### Aşama 3: EDA (Exploratory Data Analysis)
**Dosya**: `eda.py`
**Yapıdığı şey**: Silver katmanında temel istatistikler ve anormallikleri tespit eder.

**Bulgular**:
- 10,095 benzersiz yorum
- 1 kategori (Electronics)
- 9,862 benzersiz kullanıcı
- 9,849 benzersiz ürün
- Hiçbir null değer yok
- Zaman dilimi: May 2, 2026, 15:34-15:47 (13 dakika)

---

### Aşama 4: Feature Engineering (Temel Features)
**Dosya**: `feature_engineering.py`
**Yapıdığı şey**: Temporal ve istatistiksel features oluşturur.

**Oluşturulan Features**:
1. `event_date` — Tarihi (Date)
2. `event_hour` — Saati (0-23)
3. `day_of_week` — Haftanın günü (1=Pzd, 7=Paz)
4. `is_weekend` — Hafta sonu mu? (0/1)
5. `user_review_count` — Kullanıcının yaptığı toplam review sayısı
6. `product_review_count` — Ürünün aldığı toplam review sayısı
7. `day_review_count` — O günde yapılan toplam review sayısı
8. `hour_review_count` — O saatte yapılan toplam review sayısı

**Çıktı**: `/tmp/delta/features/review_features` (Delta Lake, 10,095 x 13 sütun)

---

### Aşama 5: NLP Sentiment Analysis (ÖNEMLİ - BURASI)
**Dosya**: `nlp_sentiment_pipeline.py`
**Yapıdığı şey**: Her yorumdan **duygu puanı** çıkarır.

#### Ne Yapıyor?

1. **Silver katmanı okunur** (temizlenmiş veriler)
2. **TextBlob Sentiment Analyzer** kullanılır
   - Her `review_body` (yorum metni) okunur
   - Polarity skoru hesaplanır: -1 (çok olumsuz) → 0 (tarafsız) → +1 (çok olumlu)
   - Normalize edilir: 0 → 1 arası

3. **Duygu Kategorisi** atanır:
   - **Negative** (0-0.33): "Kötü ürün", "Yaramadı"
   - **Neutral** (0.33-0.67): "Fena değil", "İdare eder"
   - **Positive** (0.67-1): "Çok iyi!", "Mükemmel"

4. **Temporal + Statistical Features** eklenir (Feature Engineering'den)

5. **Feature Store'a yazılır** (`/tmp/delta/features/review_features_with_sentiment`)

#### Çalıştırma

```bash
python nlp_sentiment_pipeline.py
```

#### Çıktı Örneği

```
timestamp | review_body | star_rating | sentiment_score | sentiment_label | user_review_count | ...
---------|------------|-----------|-----------------|-----------------|-------------------|----
1746... | "Great!" | 5 | 0.92 | Positive | 3 | ...
1746... | "Broken" | 1 | 0.15 | Negative | 1 | ...
1746... | "OK" | 3 | 0.50 | Neutral | 2 | ...
```

**Özet İstatistikler**:
```
Positive:  ~6000 reviews
Neutral:   ~2500 reviews
Negative:  ~1500 reviews
```

---

## 💡 ML Modeli İçin Hazırlanacak Veriler

### Input Features (ML Modeline Verilecekler)
```python
features_for_ml = [
    # Duygu (NLP)
    'sentiment_score',        # 0-1 arası
    'star_rating',            # 1-5 arası
    
    # Temporal
    'event_hour',             # 0-23
    'day_of_week',            # 1-7
    'is_weekend',             # 0 veya 1
    
    # İstatistiksel (Kullanıcı/Ürün Davranışı)
    'user_review_count',      # Bu kullanıcı kaç review yaptı?
    'product_review_count',   # Bu ürün kaç review aldı?
    'day_review_count',       # O gün kaç review yapıldı?
    'hour_review_count',      # O saat kaç review yapıldı?
]

# Target (Kümeleme için)
# clustering_label = KMeans(n_clusters=3).fit_predict(features_for_ml)
```

### Veri Kaynağı
**Delta Lake Feature Store**: `/tmp/delta/features/review_features_with_sentiment`

```python
spark.read.format("delta").load("/tmp/delta/features/review_features_with_sentiment").show()
```

---

## 🎯 Arkadaşınızın Yapması Gereken (ML Aşaması)

### Adım 1: Feature Store'dan Veriyi Oku
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MLModel").getOrCreate()
feature_df = spark.read.format("delta").load("/tmp/delta/features/review_features_with_sentiment")
feature_df.show()
```

### Adım 2: Kümeleme (Clustering)
```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# Features'ı Vector'e dönüştür
assembler = VectorAssembler(
    inputCols=[
        'sentiment_score', 'star_rating', 'event_hour', 
        'day_of_week', 'is_weekend', 'user_review_count',
        'product_review_count', 'day_review_count', 'hour_review_count'
    ],
    outputCol="features"
)

data = assembler.transform(feature_df)

# K-Means Clustering (3 cluster)
kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(data)
predictions = model.transform(data)

predictions.select("sentiment_label", "prediction").show()
```

### Adım 3: Sonuçları Analiz Et
```python
predictions.groupBy("sentiment_label", "prediction").count().show()
```

---

## 📁 Proje Dosya Yapısı

```
amazon_big_data_project/
├── producer/
│   └── producer.py              # Kafka'ya veri gönder
├── spark_streaming/
│   └── spark_streaming.py       # Bronze/Silver/Gold streams
├── nlp_sentiment_pipeline.py    # ⭐ Sentiment analizi
├── feature_engineering.py        # Temporal + istatistiksel features
├── eda.py                        # Exploratory Data Analysis
├── docker-compose.yml           # Kafka, Spark, Zookeeper
├── data/
│   └── amazon_reviews_us_Electronics_v1_00_sample.csv
└── notebooks/
```

---

## 🚀 Çalıştırma Sırası

1. **Docker başlat**:
   ```bash
   docker-compose up -d
   ```

2. **Producer çalıştır** (Kafka'ya veri gönder):
   ```bash
   python producer/producer.py --rate 20
   ```

3. **Spark Streaming çalıştır** (Bronze/Silver/Gold):
   ```bash
   docker exec spark-notebook python /home/jovyan/spark_streaming/spark_streaming.py
   ```

4. **NLP Pipeline çalıştır** (Sentiment analizi):
   ```bash
   docker exec spark-notebook python /home/jovyan/nlp_sentiment_pipeline.py
   ```

5. **Arkadaşınız ML modelini çalıştırır**:
   ```bash
   python ml_model.py
   ```

---

## 🔑 Önemli Noktalar

- **NLP Pipeline'ın çıktısı**: Feature Store (`/tmp/delta/features/review_features_with_sentiment`)
- **ML Modeli için input**: Sentiment score + diğer features
- **Duygu kategorileri**: Negative (olumsuz), Neutral (tarafsız), Positive (olumlu)
- **Hedef**: Kullanıcıları sentiment ve davranış açısından segmentlendirmek (kümeleme)

---

## 📝 Notlar

- TextBlob İngilizce ve kısmen Türkçe desteği var
- Daha hassas sonuçlar için BERT (Transformers) kullanılabilir
- Star rating ile sentiment score arasında korelasyon beklenir
- Ürün kategorisine göre sentiment analizi filtrelenebilir

---

**Hazırlayan**: Data Engineering Team  
**Tarih**: May 2, 2026  
**Versiyon**: 1.0
