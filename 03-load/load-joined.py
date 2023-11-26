import csv
import os
from dotenv import load_dotenv
from psycopg2 import connect

load_dotenv()

db_host = os.environ.get("localhost")
db_name = os.environ.get("weather_rekdat")
db_user = os.environ.get("")
db_password = os.environ.get("")

try:
    conn = connect(
        host='localhost',
        database='weather_rekdat',
        user='postgres',
        password='1816',
        port=5432
    )
    print("Connected to the database.")

    def merge_data(conn, bmkg_file, iqair_file):
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
        # available dates for bmkg
        bmkg_available_dates = [
            "2023-11-19",
            "2023-11-20",
            "2023-11-21",
            "2023-11-22",
            "2023-11-23",
            "2023-11-24"
        ]

        # available dates for iqair
        iqair_available_dates = [
            "2023-11-20",
            "2023-11-21",
            "2023-11-22",
            "2023-11-23",
            "2023-11-24",
            "2023-11-25"
        ]

        # Change the available_date to select the desired date
        available_date = "2023-11-20"

        if available_date in bmkg_available_dates and available_date in iqair_available_dates:
            bmkg_csv_file = f"/Users/erikuncoro/Documents/Project_Rekdat/ETL-Pipeline/csv/bmkg/{available_date}.csv"
            iqair_csv_file = f"/Users/erikuncoro/Documents/Project_Rekdat/ETL-Pipeline/csv/iqair/{available_date}.csv"

            merge_data(conn, bmkg_csv_file, iqair_csv_file)
            print(f"Data for {available_date} has been merged and inserted into join_table.")
        else:
            print(f"No data available for {available_date} in both bmkg and iqair.")

except Exception as e:
    print(f"Error: {e}")
finally:
    if conn:
        conn.close()
