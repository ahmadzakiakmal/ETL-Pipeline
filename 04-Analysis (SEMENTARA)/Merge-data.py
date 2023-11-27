import csv

# Fungsi untuk menggabungkan data dari dua file CSV
def merge_data(bmkg_file, iqair_file):
    bmkg_data = {}
    iqair_data = {}

    # Membaca data dari file BMKG
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

    # Membaca data dari file IQAir
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

    # Menggabungkan data yang sesuai
    joined_data = []
    for id, bmkg_info in bmkg_data.items():
        if id in iqair_data:
            iqair_info = iqair_data[id]
            joined_data.append({
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
            })

    return joined_data

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

# Memproses setiap tanggal yang tersedia
for available_date in bmkg_available_dates:
    if available_date in iqair_available_dates:
        bmkg_csv_file = f"/Users/erikuncoro/Documents/Project_Rekdat/ETL-Pipeline/csv/bmkg/{available_date}.csv"
        iqair_csv_file = f"/Users/erikuncoro/Documents/Project_Rekdat/ETL-Pipeline/csv/iqair/{available_date}.csv"

        # Panggil fungsi untuk menggabungkan data
        merged_data = merge_data(bmkg_csv_file, iqair_csv_file)

        # Lokasi file CSV untuk hasil gabungan
        output_csv_file = f"/Users/erikuncoro/Documents/Project_Rekdat/ETL-Pipeline/csv/merged_data_{available_date}.csv"

        # Menyimpan hasil gabungan data ke dalam file CSV baru
        with open(output_csv_file, mode='w', newline='') as file:
            fieldnames = ['id', 'kota', 'jamCuaca', 'kodeCuaca', 'cuaca', 'humidity', 'tempC', 'tempF',
                          'iqa', 'wind_dir_deg', 'wind_spd_km_h', 'pressure_mbar', 'accessed']
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            writer.writeheader()
            for data in merged_data:
                writer.writerow(data)

        print(f"Data for {available_date} has been saved to {output_csv_file}")
    else:
        print(f"No data available for {available_date} in both bmkg and iqair.")
