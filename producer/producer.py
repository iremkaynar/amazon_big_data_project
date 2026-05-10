import argparse
import json
import os
import signal
import sys
import time
import pandas as pd
from kafka import KafkaProducer
import logging

# Producer loglarını ekranda görmek için ayar yapıyoruz
logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser(description="Kafka producer for streaming CSV rows as JSON")
    parser.add_argument("--rate", type=int, help="messages per second (overrides MESSAGE_PER_SEC env)")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:29092"),
                        help="Kafka bootstrap servers (host:port)")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "amazon_reviews_topic"),
                        help="Kafka topic to send messages to")
    args = parser.parse_args()

    # Determine message rate (messages per second)
    rate = args.rate if args.rate is not None else int(os.getenv("MESSAGE_PER_SEC", "20"))
    sleep_sec = 1.0 / rate if rate > 0 else 0

    # Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Read CSV
    file_electronics = "data/amazon_reviews_us_Electronics_v1_00_sample.csv"
    file_videogames = "data/amazon_reviews_us_Digital_Video_Games_v1_00_sample.csv"

    # İkisini de oku
    df_elec = pd.read_csv(file_electronics)
    df_vg = pd.read_csv(file_videogames)

    # Alt alta birleştir
    df = pd.concat([df_elec, df_vg], ignore_index=True)

    # Karıştır (Verilerin kategoriler arası karışık gitmesi model eğitimi için daha sağlıklıdır)
    df = df.sample(frac=1).reset_index(drop=True)
    

    sent = 0

    def handle_sigint(signum, frame):
        logging.info("SIGINT received, flushing producer and exiting...")
        try:
            producer.flush(timeout=10)
        except Exception:
            pass
        logging.info(f"Toplam gönderilen mesaj: {sent}")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    for index, row in df.iterrows():
        mesaj = {
            "timestamp": time.time(),
            "kullanici_ID": str(row.get("customer_id", "BilinmeyenKullanici")),
            "olay_tipi": "review_posted",
            "ilgili_ID": str(row.get("product_id", "BilinmeyenUrun")),
            "kategori": str(row.get("product_category", "BilinmeyenKategori")),
            "star_rating": int(row.get("star_rating", 0)),
            "review_body": str(row.get("review_body", "")),
            "review_headline": str(row.get("review_headline", "")),
            "helpful_votes": int(row.get("helpful_votes", 0))
        }

        producer.send(args.topic, mesaj)
        sent += 1
        logging.info(f"Mesaj Gönderildi ({sent}): Kullanıcı {mesaj['kullanici_ID']} -> Ürün {mesaj['ilgili_ID']}")

        if sleep_sec > 0:
            time.sleep(sleep_sec)

    # Finish: flush producer
    try:
        producer.flush(timeout=10)
    except Exception:
        pass
    logging.info(f"Gönderim tamamlandı. Toplam mesaj: {sent}")


if __name__ == "__main__":
    main()