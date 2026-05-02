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
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
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
    df = pd.read_csv("data/amazon_reviews_us_Electronics_v1_00_sample.csv")

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
            "review_body": str(row.get("review_body", ""))
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