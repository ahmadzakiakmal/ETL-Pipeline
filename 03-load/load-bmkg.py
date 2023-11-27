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
# date = "2023-11-26"

# Coba koneksi
try:
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user="postgres",
        password=os.environ.get("DB_PASSWORD"),
        port=5432,
    )
    cur = conn.cursor()
    print("Connected to database.")
    connected = True
except psycopg2.Error as e:
    print(f"Failed to connect to database: {e}")

# Lanjutkan dengan kode untuk memuat file jika koneksi berhasil
if connected:
    file_path = f"../csv/bmkg/{date}.csv"

    if os.path.exists(file_path):
        print(f"Copying {date}.csv to bmkg table...")
        # Memuat file hanya jika file ada
        with open(file_path, "r", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            data_to_insert = [
                {
                    "id": row["id"],
                    "kota": row["kota"],
                    "jamCuaca": row["jamCuaca"],
                    "kodeCuaca": int(row["kodeCuaca"]),
                    "cuaca": row["cuaca"],
                    "humidity": int(row["humidity"]),
                    "tempC": float(row["tempC"]),
                    "tempF": float(row["tempF"]),
                }
                for row in reader
            ]

            cur = conn.cursor()
            status = "ERROR"
            try:
                placeholders = ", ".join(["%s"] * len(data_to_insert[0]))
                columns = ", ".join(data_to_insert[0].keys())
                query = f"INSERT INTO bmkg ({columns}) VALUES ({placeholders})"
                cur.executemany(
                    query, [tuple(data.values()) for data in data_to_insert]
                )
                conn.commit()
                print(f"Successfully copied {date}.csv to bmkg table.")
                status = "SUCCESS"
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Error copying {date}.csv to bmkg table: {e}")
            finally:
                cur.close()
                # # make a log.txt file and log loading event
                date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with open("../logs/bmkg.txt", "a") as log_file:
                    log_file.write(
                        f"{date_time} - Loading BMKG Data {date}.csv - [{status}]\n"
                    )
    else:
        print(f"No file found for {date}.csv")
