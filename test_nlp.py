"""
NLP Sentiment Test - Hızlı Test Script
CSV'den doğrudan review'ları alıp sentiment analizi yapar
"""
import pandas as pd
from textblob import TextBlob

def get_sentiment_score(text):
    """TextBlob ile sentiment analizi"""
    if text is None or text == "":
        return 0.0
    try:
        blob = TextBlob(str(text))
        polarity = blob.sentiment.polarity
        normalized = (polarity + 1) / 2
        return round(normalized, 3)
    except:
        return 0.5

def get_sentiment_label(score):
    """Score'u kategoriye çevir"""
    if score < 0.33:
        return "Negative"
    elif score < 0.67:
        return "Neutral"
    else:
        return "Positive"

# CSV'yi oku
print("📖 CSV dosyası okunuyor...")
df = pd.read_csv("data/amazon_reviews_us_Electronics_v1_00_sample.csv")

# Test: İlk 5 yorum
print(f"\n🎯 İlk 5 Yorum - Sentiment Analizi:\n")
print("-" * 100)

test_df = df[['review_body', 'star_rating']].head(5).copy()
test_df['sentiment_score'] = test_df['review_body'].apply(get_sentiment_score)
test_df['sentiment_label'] = test_df['sentiment_score'].apply(get_sentiment_label)

for idx, row in test_df.iterrows():
    print(f"⭐ Yıldız: {row['star_rating']} | Skor: {row['sentiment_score']} | {row['sentiment_label']}")
    print(f"   Yorum: {row['review_body'][:70]}")
    print()

# Özet İstatistikler (tüm veriyi işle)
print("-" * 100)
print("\n📊 ÖZET İSTATİSTİKLER (Tüm 10,095 Yorum):\n")

df['sentiment_score'] = df['review_body'].apply(get_sentiment_score)
df['sentiment_label'] = df['sentiment_score'].apply(get_sentiment_label)

# Duygu dağılımı
print("Duygu Dağılımı:")
print(df['sentiment_label'].value_counts())

print(f"\nSentiment Score İstatistikleri:")
print(f"  Min: {df['sentiment_score'].min():.3f}")
print(f"  Max: {df['sentiment_score'].max():.3f}")
print(f"  Ortalama: {df['sentiment_score'].mean():.3f}")
print(f"  Medyan: {df['sentiment_score'].median():.3f}")

# Star rating vs sentiment korelasyonu
print(f"\n⚡ Star Rating vs Sentiment Korelasyonu:")
for star in sorted(df['star_rating'].unique()):
    avg_sentiment = df[df['star_rating'] == star]['sentiment_score'].mean()
    count = len(df[df['star_rating'] == star])
    print(f"  {star} Yıldız: Avg Sentiment = {avg_sentiment:.3f} ({count} yorum)")

print("\n✅ NLP Sentiment Pipeline Başarıyla Çalışıyor!")
print("   ✓ Review body okunuyor")
print("   ✓ Sentiment skoru hesaplanıyor (0-1)")
print("   ✓ Kategorize ediliyor (Negative/Neutral/Positive)")
print("   ✓ Feature Engineering'e hazır!")
