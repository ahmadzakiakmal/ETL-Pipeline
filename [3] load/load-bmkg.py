import psycopg2
import csv
import os
from dotenv import load_dotenv
from datetime import datetime

# db credentials
load_dotenv()
db_host = os.environ.get("DB_HOST")
db_name = os.environ.get("DB_NAME")
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")

date = datetime.now().strftime("%Y-%m-%d")
file_names = []
for i in range(0,4):
    file_names.append(f"csv/raw-iqair/{date}-{i*6}.csv")
    
conn = psycopg2.connect(
    host=db_host,
    database=db_name,
    user=db_user,
    password=db_password,
    port=5432,
)

# try connection
connected = False
try:
    cur = conn.cursor()
    print("Connected to database.")
    connected = True
except:
    print("Failed to connect to database.")
    
# if connected:
#     with open(f"csv/raw-bmkg/{date}.csv") as csv_file:
#         cur = conn.cursor()
#         cur.copy_from(csv_file, "bmkg", sep=",")
#         conn.commit()
#         cur.close()
#         print(f"Successfully copied {date}.csv to bmkg table.")


