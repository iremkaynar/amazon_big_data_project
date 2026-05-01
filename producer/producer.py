import json
import time
import pandas as pd
from kafka import KafkaProducer
import logging

# Producer loglarını ekranda görmek için ayar yapıyoruz
logging.basicConfig(level=logging.INFO)

# Kafka'ya bağlanıyoruz
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', # Docker'daki Kafka portumuz
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Örneklem verisini okuyoruz (Dosya yolunu kendi yapınıza göre güncelle)
df = pd.read_csv("data/amazon_reviews_us_Electronics_v1_00_sample.csv")

for index, row in df.iterrows():
    # Proje isterine uygun JSON formatını oluşturuyoruz
    mesaj = {
        "timestamp": time.time(),
        "kullanici_ID": str(row.get("reviewerID", "BilinmeyenKullanici")),
        "olay_tipi": "review_posted",
        "ilgili_ID": str(row.get("asin", "BilinmeyenUrun")),
        "kategori": str(row.get("category", "BilinmeyenKategori"))
    }
    
    # Mesajı Kafka'ya gönderiyoruz
    producer.send('amazon_reviews_topic', mesaj)
    logging.info(f"Mesaj Gönderildi ({index + 1}): Kullanıcı {mesaj['kullanici_ID']} -> Ürün {mesaj['ilgili_ID']}")
    
    # Saniyede yaklaşık 20 mesaj gönderim hızı (10-100 kuralına uygun)
    time.sleep(0.05)