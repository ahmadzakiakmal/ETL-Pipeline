import pandas as pd

file_path = './csv/raw-iqair/2023-11-20-06.csv'

df = pd.read_csv(file_path)

df['kota'] = df['kota'].astype(str)

df['wind_dir(deg)'] = df['wind_dir(deg)'].str.replace('derajat', '')
df['wind_dir(deg)'] = df['wind_dir(deg)'].str.replace('Angin berputar', '')
df['wind_dir(deg)'] = df['wind_dir(deg)'].astype(int)

df['wind_spd(km/h)'] = df['wind_spd(km/h)'].str.replace('km/h', '')
df['wind_spd(km/h)'] = df['wind_spd(km/h)'].astype(float)

df['pressure(mbar)'] = df['pressure(mbar)'].str.replace('mbar', '')
df['pressure(mbar)'] = df['pressure(mbar)'].astype(int)

df['iqa'] = df['iqa'].str.replace('*', '')
df['iqa'] = df['iqa'].astype(int)

df['accessed'] = pd.to_datetime(df['accessed'], format = '%Y-%m-%d %H')

print(df.head())
print(df.dtypes)

cleaned_file_path = './csv/cleaned-bmkg/2023-11-19_cleaned.csv'
df.to_csv(cleaned_file_path, index=False)
