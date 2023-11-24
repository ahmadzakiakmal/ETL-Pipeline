import pandas as pd
from datetime import datetime

# date = datetime.today().strftime('%Y-%m-%d')
date = "2023-11-21"
# Path file CSV
file_path = f"../csv/raw-bmkg/{date}.csv"

# Membaca file CSV ke dalam DataFrame
df = pd.read_csv(file_path)

# Mengonversi datatype menjadi datetime
df["jamCuaca"] = pd.to_datetime(df["jamCuaca"], format="%Y-%m-%d %H:%M:%S")

# Memeriksa duplikat berdasarkan kolom 'id'
duplicate_rows = df[df.duplicated(["id"])]

# Menampilkan baris yang merupakan duplikat
# print("Duplikat berdasarkan seluruh kolom:")
print("duplicates:",len(duplicate_rows))

# Sort berdasarkan kolom 'id'
df.sort_values(by=["id"], inplace=True)

# Menghapus duplikat (menyimpan baris pertama yang ditemukan)
cleaned_df = df.drop_duplicates(subset=['id'])

# Menyimpan DataFrame yang sudah bersih ke dalam file CSV baru
cleaned_file_path = f"../csv/cleaned-bmkg/{date}.csv"
cleaned_df.to_csv(cleaned_file_path, index=False)
duplicated2 = cleaned_df[cleaned_df.duplicated(["id"])]
print("duplicates2:",len(duplicated2))

# Menampilkan informasi setelah menghapus duplikat
# print("\nDataFrame setelah menghapus duplikat:")
# print(cleaned_df)
