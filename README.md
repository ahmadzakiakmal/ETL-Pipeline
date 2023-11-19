# Weather and Air Quality Data Pipeline
End-to-end data ETL pipeline for **weather data from BMKG** and **air quality data from IQAIR**

# ETL Process

## Extraction
1. extract data from BMKG's API using `scrape-bmkg.py`, schedule once daily.
2. extract data from IQAIR's Website using `scrape-iqair.py`, schedule four times a day: **00:00**, **06:00**, **12:00**, **18:00**.

## Transform
Clean the data from IQAir.

## Load
Load data into PostgreSQL database.
