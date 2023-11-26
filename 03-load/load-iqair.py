import psycopg2
import csv
import os
from dotenv import load_dotenv
from datetime import datetime
from psycopg2 import extras

# db credentials
load_dotenv()
db_host = os.environ.get("localhost")
db_name = os.environ.get("weather_rekdat")
db_user = os.environ.get("")
db_password = os.environ.get("")

date = datetime.now().strftime("%Y-%m-%d")

# Coba koneksi
try:
    conn = psycopg2.connect(
        host="localhost",
        database="weather_rekdat",
        user="postgres",
        password="1816",
        port=5432,
    )
    cur = conn.cursor()
    print("Connected to database.")
    connected = True
except psycopg2.Error as e:
    print(f"Failed to connect to database: {e}")

# Lanjutkan dengan kode untuk memuat file jika koneksi berhasil
if connected:
    available_dates = [
        "2023-11-20",
        "2023-11-21",
        "2023-11-22",
        "2023-11-23",
        "2023-11-24",
        "2023-11-25"
    ]
    
    for available_date in available_dates:
        file_path = f"/Users/erikuncoro/Documents/Project_Rekdat/ETL-Pipeline/csv/iqair/{available_date}.csv"
        
        #id,kota,iqa,wind_dir(deg),wind_spd(km/h),pressure(mbar),accessed
        if os.path.exists(file_path):
            # Memuat file hanya jika file ada
            with open(file_path, 'r', newline='') as csv_file:
                reader = csv.DictReader(csv_file)
                data_to_insert = [{
                    'id': row['id'],
                    'kota': row['kota'],
                    'iqa': int(row['iqa']),
                    'wind_dir_deg': int(row['wind_dir(deg)']),
                    'wind_spd_km_h': float(row['wind_spd(km/h)']),
                    'pressure_mbar': int(row['pressure(mbar)']),
                    'accessed': row['accessed'],
                } for row in reader]

                cur = conn.cursor()
                try:
                    placeholders = ', '.join(['%s'] * len(data_to_insert[0]))
                    columns = ', '.join(data_to_insert[0].keys())
                    query = f"INSERT INTO iqair ({columns}) VALUES ({placeholders})"
                    cur.executemany(query, [tuple(data.values()) for data in data_to_insert])
                    conn.commit()
                    print(f"Successfully copied {available_date}.csv to iqair table.")
                except psycopg2.Error as e:
                    conn.rollback()
                    print(f"Error copying {available_date}.csv to iqair table: {e}")
                finally:
                    cur.close()
        else:
            print(f"No file found for {available_date}.csv")
