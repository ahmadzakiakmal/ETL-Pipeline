# Weather and Air Quality Data Pipeline
End-to-end data ETL pipeline for **weather data from BMKG** and **air quality data from IQAIR**

- [ETL Process Overview](#etl-process-overview)
  - [Extraction](#extraction)
  - [Transform](#transform)
  - [Load](#load)
- [Repository Structure](#repository-structure)

<a name="etl-process" />

# ETL Process Overview

![image](https://github.com/ahmadzaki2975/ETL-Pipeline/assets/87590846/878a8d3a-fb8b-4219-b7c2-5d866a46b7b1)

## Extraction
1. extract data from BMKG's API using `scrape-bmkg.py`, schedule once daily.
2. extract data from IQAIR's Website using `scrape-iqair.py`, schedule four times a day: **00:00**, **06:00**, **12:00**, **18:00**.

## Transform
Clean the data from IQAir.

## Load
Load data into PostgreSQL database.


# Repository Structure
```
📦 
├─ .env
├─ .gitignore
├─ 01-extract
│  ├─ bat
│  │  ├─ commit-bmkg.bat
│  │  ├─ commit-iqr.bat
│  │  ├─ scrape-bmkg.bat
│  │  └─ scrape-iqair.bat
│  ├─ scrape-bmkg.py
│  └─ scrape-iqair.py
├─ 02-transform
│  ├─ bat
│  │  ├─ commit-bmkg.bat
│  │  ├─ commit-iqair.bat
│  │  ├─ transform-bmkg.bat
│  │  └─ transform-iqair.bat
│  ├─ transform-bmkg.py
│  └─ transform-iqair.py
├─ 03-load
│  ├─ load-bmkg.py
│  └─ load-iqair.py
├─ Cleaning-bmkg-raw.py
├─ Cleaning-iqair-raw.py
├─ LICENSE
├─ README.md
├─ csv
│  ├─ bmkg
│  │  ├─ [CSVs of final BMKG data]
│  ├─ cleaned-iqair
│  │  ├─ [CSVs of cleaned IQAir data]
│  ├─ iqair
│  │  ├─ [CSVs of final IQAir data]
│  ├─ raw-bmkg
│  │  ├─ [CSVs of raw BMKG data]
│  └─ raw-iqair
│     ├─ [CSVs of raw IQAir data]
├─ endpoint-wilayah.txt
└─ json
   └─ wilayah.json
```
