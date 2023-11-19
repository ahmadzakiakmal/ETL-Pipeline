# BMKG API Scraper
# Ahmad Zaki Akmal
# All data is owned by BMKG

# Libs
import requests
import json
import csv
from datetime import datetime
import datetime as dt

# BMKG API Base URL
baseUrl = "https://ibnux.github.io/BMKG-importer/"

# List of regions
import requests
import json

# Function to get region's weather data
def getWeatherData (kota ,regionId, inputDate):
  # Get weather data
  weatherUrl = baseUrl + "cuaca/" + regionId + ".json"
  print("Getting data from " + kota)
  if(regionId == "0") :
    return None
  
  def format_id(kota, date):
    # Remove "Kota " or "Kab. "
    cleaned_city_name = kota.replace("Kota ", "").replace("Kab. ", "")

    # Convert the date string to a datetime object
    date_obj = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

    # Format the date as ISO 8601
    formatted_date = date_obj.isoformat()

    # Concatenate city name and formatted date to create the ID
    unique_id = f"{cleaned_city_name}_{formatted_date}"
    return unique_id
  
  try:
    weatherData = requests.get(weatherUrl)
    # Check if the request was successful
    weatherData.raise_for_status()
    weatherData = weatherData.json()  # Parse JSON data directly
    # If the data's jamCuaca does not contain inputDate, remove the sub data
    weatherData = [data for data in weatherData if data["jamCuaca"].startswith(str(inputDate))]
    # Append extra attribute to each dictionary in weatherData
    for data in weatherData:
        tempKota = kota.replace("Kota ", "").replace("Kab. ", "")
        data["kota"] = tempKota.strip()
        data["id"] = format_id(tempKota, data["jamCuaca"])
    return weatherData
  except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
    return None
  except json.JSONDecodeError as json_err:
    print(f"JSON decoding error occurred: {json_err}")
    return None
  except requests.exceptions.RequestException as req_err:
    print(f"Request error occurred: {req_err}")
    return None
  except Exception as err:
    print(f"An error occurred (getWeatherData): {err}")
    return None
  
today = dt.date.today()
# format today's date to YYYY-MM-DD
inputDate = today.strftime("%Y-%m-%d")

try:
    # read json
    with open("json/wilayah.json", "r") as jsonFile:
      listOfRegions = json.load(jsonFile)
    
    # Keys to extract
    regionKeys = ["id", "propinsi", "kota"]
    filteredRegions = []

    # Iterate over the loaded JSON data
    for region in listOfRegions:
      regionDict = {}
      for key in regionKeys:
          regionDict[key] = region.get(key)
      filteredRegions.append(regionDict)
      
    # ! FOR DEBUGGING: trim to only first 10 regions
    filteredRegions = filteredRegions[:10]
      
    # Iterate each region and call their respective API    
    weatherData = []
    for region in filteredRegions:
      tempWeatherData = getWeatherData(region["kota"] ,region["id"], today)
      if tempWeatherData:
        weatherData.append(tempWeatherData)
        
    # Write to CSV  
    date = datetime.now().strftime("%Y-%m-%d %H:%M")
    date = datetime.strptime(date, '%Y-%m-%d %H:%M')
    date = date.strftime("%Y-%m-%d")
    csvHeader = ["id", "kota", "jamCuaca", "kodeCuaca", "cuaca", "humidity", "tempC", "tempF"]
    
    with open(f"csv/raw-bmkg/{date}.csv", "w") as csvFile:
      csvWriter = csv.DictWriter(csvFile, fieldnames=csvHeader)
      csvWriter.writeheader()
      iter = 0
      for data in weatherData:
          for subData in data:
            iter += 1
            csvWriter.writerow(subData)
            
    print("Written " + str(iter) + " rows to CSV")

except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except json.JSONDecodeError as json_err:
    print(f"JSON decoding error occurred: {json_err}")
except requests.exceptions.RequestException as req_err:
    print(f"Request error occurred: {req_err}")
except Exception as err:
    print(f"An error occurred (Write CSV): {err}")




