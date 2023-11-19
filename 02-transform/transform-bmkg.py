import csv
import pandas as pd

# get a dataframe from csv
df = pd.read_csv("csv/raw-bmkg/2023-11-19.csv")

print(df.info())
print(df.duplicated().sum())

