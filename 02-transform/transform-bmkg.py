import pandas as pd

# Path file CSV
file_path = '/Users/erikuncoro/Documents/Project_Rekdat/ETL-Pipeline/csv/raw-bmkg/2023-11-19.csv'

# Membaca file CSV ke dalam DataFrame
df = pd.read_csv(file_path)

# Memeriksa duplikat berdasarkan seluruh kolom
duplicate_rows = df[df.duplicated()]

# Menampilkan baris yang merupakan duplikat
print("Duplikat berdasarkan seluruh kolom:")
print(duplicate_rows)

# Menghapus duplikat (menyimpan baris pertama yang ditemukan)
cleaned_df = df.drop_duplicates()

# Menyimpan DataFrame yang sudah bersih ke dalam file CSV baru
cleaned_file_path = '/Users/erikuncoro/Documents/Project_Rekdat/ETL-Pipeline/csv/cleaned-bmkg/2023-11-19_cleaned.csv'
cleaned_df.to_csv(cleaned_file_path, index=False)

# Menampilkan informasi setelah menghapus duplikat
print("\nDataFrame setelah menghapus duplikat:")
print(cleaned_df)
