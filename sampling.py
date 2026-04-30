import pandas as pd
import os
import subprocess

# 1. Ayarlar
dataset = "cynthiarempel/amazon-us-customer-reviews-dataset"
output_dir = "data"

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# 2. Örneklem Alınacak Dosyalar (Hepsini değil, en popüler 3 kategoriyi seçelim)
# Duygu analizi ve partition analizi için bu kategoriler idealdir.
files_to_sample = [
    "amazon_reviews_us_Electronics_v1_00.tsv",
    "amazon_reviews_us_Books_v1_00.tsv",
    "amazon_reviews_us_Digital_Video_Games_v1_00.tsv"
]

print(f"--- Örneklem Alma İşlemi Başladı ---")

for file_name in files_to_sample:
    print(f"\nİşleniyor: {file_name}")
    
    # Kaggle API ile dosyayı sadece ilk birkaç bin satırı alacak şekilde indirmek zor olduğu için, 
    # dosyayı zip olarak indirip hızlıca okuyup geri sileceğiz.
    try:
        subprocess.run(["kaggle", "datasets", "download", "-d", dataset, "-f", file_name, "-p", output_dir], check=True)
        
        zip_path = os.path.join(output_dir, file_name + ".zip")
        
        # Amazon veri seti TSV (Tab-Separated) formatındadır.
        # İlk 10.000 satırı okuyalım.
        df_sample = pd.read_csv(zip_path, sep='\t', nrows=10000, on_bad_lines='skip')
        
        # Örneklemi CSV olarak kaydet
        sample_name = f"{file_name.replace('.tsv', '')}_sample.csv"
        df_sample.to_csv(os.path.join(output_dir, sample_name), index=False)
        
        # Yer kaplamasın diye orijinal zip'i sil
        if os.path.exists(zip_path):
            os.remove(zip_path)
            
        print(f"BAŞARILI: {sample_name} oluşturuldu.")
        
    except Exception as e:
        print(f"HATA: {file_name} işlenirken bir sorun oluştu: {e}")

print(f"\n--- Tüm İşlemler Tamamlandı. Veriler '{output_dir}' klasöründe! ---")