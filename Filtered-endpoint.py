# Fungsi untuk membersihkan dan menyimpan data ke file
def save_clean_data(data, output_file):
    with open(output_file, 'w') as file:
        for item in data:
            file.write("%s\n" % item)

# Baca data dari file endpoint-wilayah.txt
with open('endpoint-wilayah.txt', 'r') as file:
    endpoint_data = file.readlines()

# Hapus petik dua dari endpoint-wilayah.txt dan simpan ke file endpoint2.txt
endpoint2_data = [line.replace('"', '').replace(',', '') for line in endpoint_data]
save_clean_data(endpoint2_data, 'endpoint2.txt')

# Baca data dari file null-regions.txt
with open('null-regions.txt', 'r') as file:
    null_regions_data = file.readlines()

# Hapus string yang sudah ada di null-regions.txt dari endpoint-wilayah.txt
filtered_data = [line for line in endpoint2_data if line not in null_regions_data]

# Simpan hasil ke file filtered.txt
save_clean_data(filtered_data, 'filtered.txt')

# Fungsi untuk membersihkan dan menyimpan data ke file
def save_clean_data(data, output_file):
    with open(output_file, 'w') as file:
        for item in data:
            file.write("\"%s\",\n" % item)

# Baca data dari file filtered.txt
with open('filtered.txt', 'r') as file:
    filtered_data = file.readlines()

# Hapus baris kosong dari filtered.txt
filtered_data = [line.strip() for line in filtered_data if line.strip()]

# Simpan hasil ke file filtered.txt
save_clean_data(filtered_data, 'filtered.txt')
