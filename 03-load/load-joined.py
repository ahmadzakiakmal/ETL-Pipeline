import csv
import os
from dotenv import load_dotenv
from psycopg2 import connect
from datetime import datetime

load_dotenv()

db_host = os.environ.get("DB_HOST")
db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
status = "ERROR"

try:
    conn = connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=5432
    )
    print("Connected to the database.")

    def merge_data(conn, bmkg_file, iqair_file):
        print("Merging data...")
        bmkg_data = {}
        iqair_data = {}

        with open(bmkg_file, 'r', newline='') as bmkg_csv:
            bmkg_reader = csv.DictReader(bmkg_csv)
            for row in bmkg_reader:
                bmkg_data[row['id']] = {
                    'kota': row['kota'],
                    'jamCuaca': row['jamCuaca'],
                    'kodeCuaca': int(row['kodeCuaca']),
                    'cuaca': row['cuaca'],
                    'humidity': int(row['humidity']),
                    'tempC': float(row['tempC']),
                    'tempF': float(row['tempF'])
                }

        with open(iqair_file, 'r', newline='') as iqair_csv:
            iqair_reader = csv.DictReader(iqair_csv)
            for row in iqair_reader:
                iqair_data[row['id']] = {
                    'kota': row['kota'],
                    'iqa': int(row['iqa']),
                    'wind_dir_deg': int(row['wind_dir(deg)']),
                    'wind_spd_km_h': float(row['wind_spd(km/h)']),
                    'pressure_mbar': int(row['pressure(mbar)']),
                    'accessed': row['accessed'],
                }

        print("Inserting data into join_table...")
        cur = conn.cursor()
        for id, bmkg_info in bmkg_data.items():
            if id in iqair_data:
                iqair_info = iqair_data[id]
                join_data = {
                    'id': id,
                    'kota': bmkg_info['kota'],
                    'jamCuaca': bmkg_info['jamCuaca'],
                    'kodeCuaca': bmkg_info['kodeCuaca'],
                    'cuaca': bmkg_info['cuaca'],
                    'humidity': bmkg_info['humidity'],
                    'tempC': bmkg_info['tempC'],
                    'tempF': bmkg_info['tempF'],
                    'iqa': iqair_info['iqa'],
                    'wind_dir_deg': iqair_info['wind_dir_deg'],
                    'wind_spd_km_h': iqair_info['wind_spd_km_h'],
                    'pressure_mbar': iqair_info['pressure_mbar'],
                    'accessed': iqair_info['accessed']
                }

                columns = ', '.join(join_data.keys())
                placeholders = ', '.join(['%s'] * len(join_data))
                query = f"INSERT INTO join_table ({columns}) VALUES ({placeholders})"
                cur.execute(query, list(join_data.values()))
                conn.commit()

    if conn:
        # # available dates for bmkg
        # bmkg_available_dates = [
        #     "2023-11-19",
        #     "2023-11-20",
        #     "2023-11-21",
        #     "2023-11-22",
        #     "2023-11-23",
        #     "2023-11-24",
        #     "2023-11-25"
        # ]

        # # available dates for iqair
        # iqair_available_dates = [
        #     "2023-11-20",
        #     "2023-11-21",
        #     "2023-11-22",
        #     "2023-11-23",
        #     "2023-11-24",
        #     "2023-11-25"
        # ]

        # Change the available_date to select the desired date
        date = "2023-11-29"

        
        bmkg_csv_file = f"../csv/bmkg/{date}.csv"
        iqair_csv_file = f"../csv/iqair/{date}.csv"

        merge_data(conn, bmkg_csv_file, iqair_csv_file)
        print(f"Data for {date} has been merged and inserted into join_table.")
        status = "SUCCESS"
            # print(f"No data available for {available_date} in both bmkg and iqair.")

except Exception as e:
    print(f"Error: {e}")
finally:
    if conn:
        conn.close()
    # log into log file
    with open("../logs/join.txt", "a") as log_file:
        log_file.write(f"{datetime.now()} - Loading IQAIR Data {date}.csv - [{status}]\n")
