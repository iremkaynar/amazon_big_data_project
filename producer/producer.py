import argparse
import json
import os
import signal
import sys
import time
import pandas as pd
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)

def main():
    parser = argparse.ArgumentParser(description="Kafka producer for Parquet data")
    parser.add_argument("--rate", type=int, default=20)
    parser.add_argument("--bootstrap", default="kafka:29092") # Docker içi ağ adı
    parser.add_argument("--topic", default="amazon_reviews_topic")
    args = parser.parse_args()

    sleep_sec = 1.0 / args.rate if args.rate > 0 else 0

    # Kafka bağlantısı
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Listenin en başındaki dosya ismi
    path = "data/amazon_final_processed.parquet/part-00000-4d410576-f08f-49ce-8a33-538533d9fd62-c000.snappy.parquet"
    logging.info("Sadece tek bir veri parçası okunuyor (RAM dostu yöntem)...")

    #tümünü okumadığı için kasmayacak
    df = pd.read_parquet(path).head(5000)
    sent = 0
    def handle_sigint(signum, frame):
        producer.flush(timeout=10)
        sys.exit(0)
    signal.signal(signal.SIGINT, handle_sigint)

    for index, row in df.iterrows():
        mesaj = {
            "timestamp": time.time(),
            "kullanici_ID": str(row.get("customer_id", "Bilinmeyen")),
            "olay_tipi": "review_posted",
            "ilgili_ID": str(row.get("product_id", "Bilinmeyen")),
            "kategori": str(row.get("product_category", "Bilinmeyen")),
            "star_rating": int(row.get("star_rating", 0)),
            "review_body": str(row.get("review_body", "")),
            "review_headline": str(row.get("review_headline", "")),
            "helpful_votes": int(row.get("helpful_votes", 0))
        }

        producer.send(args.topic, mesaj)
        sent += 1
        if sent % 100 == 0:
            logging.info(f"Gönderilen mesaj sayısı: {sent}")

        time.sleep(sleep_sec)

    producer.flush(timeout=10)
    logging.info(f"İşlem bitti. Toplam: {sent}")

if __name__ == "__main__":
    main()