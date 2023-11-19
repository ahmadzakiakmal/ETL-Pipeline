import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

# list endpoints to scrape
endpoints = [
    "/aceh/banda-aceh",
    "/aceh/langsa",
    "/aceh/lhokseumawe",
    "/aceh/sabang",
    "/bali/bangli",
    "/bali/denpasar",
    "/bali/gianyar",
    "/bali/badung",
    "/bali/klungkung",
    "/bali/buleleng",
    "/bali/tabanan",
    "/banten",
    "/banten/pandeglang",
    "/banten/serang",
    "/banten/tangerang",
    "/banten/serang",
    "/banten/south-tangerang",
    "/bengkulu/bengkulu-city",
    "/bengkulu/kepahiang",
    "/yogyakarta/bantul",
    "/yogyakarta/sleman",
    "/yogyakarta/yogyakarta",
    "/gorontalo/gorontalo",
    "/gorontalo/gorontalo",
    "/jambi/jambi-city",
    "/jambi/muara-sabak",
    "/jambi/sungai-penuh",
    "/jambi/sarolangun",
    "/west-java/bandung",
    "/west-java/banjar",
    "/west-java/ciamis",
    "/west-java/cianjur",
    "/west-java/bogor",
    "/west-java/bekasi",
    "/west-java/cimahi",
    "/west-java/cirebon",
    "/west-java/sukabumi",
    "/west-java/garut",
    "/west-java/indramayu",
    "/west-java/karawang",
    "/west-java/kuningan",
    "/west-java/majalengka",
    "/west-java/sukabumi",
    "/west-java/purwakarta",
    "/west-java/tasikmalaya",
    "/west-java/bandung",
    "/west-java/subang",
    "/west-java/cirebon",
    "/west-java/sumedang",
    "/west-java/tasikmalaya",
    "/west-java/bogor",
    "/west-java/bekasi",
    "/west-java/bogor",
    "/west-java/cianjur",
    "/central-java/banjarnegara",
    "/central-java/batang",
    "/central-java/blora",
    "/central-java/boyolali",
    "/central-java/brebes",
    "/central-java/cilacap",
    "/central-java/demak",
    "/central-java/jepara",
    "/central-java/pekalongan",
    "/central-java/karanganyar",
    "/central-java/kebumen",
    "/central-java/kendal",
    "/central-java/klaten",
    "/central-java/kudus",
    "/central-java/magelang",
    "/central-java/magelang",
    "/central-java/pati",
    "/central-java/pekalongan",
    "/central-java/pemalang",
    "/central-java/purbalingga",
    "/central-java/banyumas",
    "/central-java/purworejo",
    "/central-java/rembang",
    "/central-java/salatiga",
    "/central-java/semarang",
    "/central-java/tegal",
    "/central-java/sragen",
    "/central-java/sukoharjo",
    "/central-java/surakarta",
    "/central-java/tegal",
    "/central-java/temanggung",
    "/central-java/semarang",
    "/central-java/wonogiri",
    "/central-java/wonosobo",
    "/central-java/semarang",
    "/east-java/bangkalan",
    "/east-java/banyuwangi",
    "/east-java/batu",
    "/east-java/blitar",
    "/east-java/bojonegoro",
    "/east-java/bondowoso",
    "/east-java/gresik",
    "/east-java/jember",
    "/east-java/jombang",
    "/east-java/kediri",
    "/east-java/malang",
    "/east-java/lamongan",
    "/east-java/lumajang",
    "/east-java/madiun",
    "/east-java/madiun",
    "/east-java/magetan",
    "/east-java/malang",
    "/east-java/mojokerto",
    "/east-java/nganjuk",
    "/east-java/pacitan",
    "/east-java/pamekasan",
    "/east-java/pasuruan",
    "/east-java/ponorogo",
    "/east-java/probolinggo",
    "/east-java/sampang",
    "/east-java/sidoarjo",
    "/east-java/situbondo",
    "/east-java/sumenep",
    "/east-java/surabaya",
    "/east-java/trenggalek",
    "/east-java/tuban",
    "/east-java/tulungagung",
    "/east-java/surabaya",
    "/east-java/kediri",
    "/east-java/mojokerto",
    "/east-java/probolinggo",
    "/east-java/blitar",
    "/east-java/pasuruan",
    "/west-kalimantan/bengkayang",
    "/west-kalimantan/ketapang",
    "/west-kalimantan/ngabang",
    "/west-kalimantan/pontianak",
    "/west-kalimantan/pontianak",
    "/west-kalimantan/sambas",
    "/west-kalimantan/sanggau",
    "/west-kalimantan/sekadau",
    "/west-kalimantan/singkawang",
    "/west-kalimantan/sintang",
    "/south-kalimantan/banjarbaru",
    "/south-kalimantan/banjarmasin",
    "/central-kalimantan/palangkaraya",
    "/central-kalimantan/pulang-pisau",
    "/central-kalimantan/sampit",
    "/east-kalimantan/balikpapan",
    "/east-kalimantan/bontang",
    "/east-kalimantan/samarinda",
    "/east-kalimantan/balikpapan",
    "/north-kalimantan/malinau",
    "/north-kalimantan/tarakan",
    "/riau-islands/tanjung-pinang",
    "/riau-islands/batam",
    "/riau-islands/batam",
    "/riau-islands/tarempa",
    "/lampung/bandar-lampung",
    "/lampung/metro",
    "/lampung/pringsewu",
    "/maluku/ambon",
    "/maluku/ambon",
    "/maluku/bula",
    "/maluku/dobo",
    "/maluku/namlea",
    "/maluku/tual",
    "/north-maluku/halmahera-timur",
    "/north-maluku/ternate",
    "/west-nusa-tenggara/bima",
    "/west-nusa-tenggara/dompu",
    "/west-nusa-tenggara/mataram",
    "/west-nusa-tenggara/bima",
    "/west-nusa-tenggara/bima",
    "/papua/jayapura",
    "/papua/nabire",
    "/papua/sarmi",
    "/papua/jayapura",
    "/papua/agats",
    "/papua/biak",
    "/papua/jayapura",
    "/papua/nabire",
    "/papua/serui",
    "/papua/jayapura",
    "/papua/nabire",
    "/papua/sarmi",
    "/west-papua/sorong",
    "/west-papua/kaimana",
    "/west-papua/manokwari",
    "/west-papua/sorong",
    "/west-papua/kaimana",
    "/west-papua/manokwari",
    "/west-papua/sorong",
    "/riau/bengkalis",
    "/riau/dumai",
    "/riau/pekanbaru",
    "/west-sulawesi/majene",
    "/west-sulawesi/mamasa",
    "/west-sulawesi/mamuju",
    "/south-sulawesi/bantaeng",
    "/south-sulawesi/barru",
    "/south-sulawesi/bulukumba",
    "/south-sulawesi/enrekang",
    "/south-sulawesi/jeneponto",
    "/south-sulawesi/maros",
    "/south-sulawesi/palopo",
    "/south-sulawesi/pinrang",
    "/south-sulawesi/sinjai",
    "/south-sulawesi/takalar",
    "/south-sulawesi/barru",
    "/central-sulawesi/buol",
    "/central-sulawesi/donggala",
    "/central-sulawesi/banggai",
    "/central-sulawesi/banggai",
    "/central-sulawesi/sigi-biromaru",
    "/southeast-sulawesi/kendari",
    "/southeast-sulawesi/kolaka",
    "/southeast-sulawesi/kendari",
    "/southeast-sulawesi/raha",
    "/north-sulawesi/bitung",
    "/north-sulawesi/manado",
    "/north-sulawesi/tomohon",
    "/west-sumatra/solok",
    "/west-sumatra/bukittinggi",
    "/west-sumatra/sijunjung",
    "/west-sumatra/padang",
    "/west-sumatra/pariaman",
    "/west-sumatra/payakumbuh",
    "/west-sumatra/solok",
    "/south-sumatra/lahat",
    "/south-sumatra/pagar-alam",
    "/south-sumatra/palembang",
    "/south-sumatra/prabumulih",
    "/north-sumatra/binjai",
    "/north-sumatra/medan",
    "/north-sumatra/sibolga",
]

# Base URL of IQAir website
url = "https://www.iqair.com/id/indonesia"
dataBuffer = []


def format_id(kota, date):
    date_obj = datetime.strptime(date, "%Y-%m-%d %H")
    print(date_obj)
    formatted_date = date_obj.isoformat()
    return kota + "_" + formatted_date

def scrape(endpoint, time=datetime.now().strftime("%Y-%m-%d %H")):
    response = requests.get(url + endpoint)
    print("Scraping " + url + endpoint)
    # Check if the request was successful (status code 200)
    try:
        if response.status_code == 200:
            # Parse HTML content
            soup = BeautifulSoup(response.text, "html.parser")
            title = soup.find("h1", class_="pagetitle__title").contents[0]
            index = soup.find("p", class_="aqi-value__value").contents[0]
            city = soup.find("a", class_="breadcrumb__item is-active").get_text()
            city = city.replace("Kota ", "").replace("Kab. ", "")
            city = city.strip()
            table_element = soup.find(
                "table", attrs={"_ngcontent-airvisual-web-c224": ""}
            )
            wind = "0 km/h"
            pressure = "0 mbar"
            for td in table_element.find_all("td"):
                if "mbar" in td.text:
                    pressure = td.text
                if "km/h" in td.text:
                    wind = td.text
            icon_parent = soup.find("img", class_="forecast-wind_icon")
            wind_dir = icon_parent.get("alt")

            # Print the extracted data
            print("=====================================")
            print("Title:", title)
            print("Index:", index)
            print("City:", city)
            print("Wind Direction:", wind_dir)
            print("Wind Speed:", wind)
            print("Wind Pressure:", pressure)
            print("Accessed at:", time)
            print("=====================================")

            # append to buffer
            dataBuffer.append(
                {
                    "id": format_id(city, time),
                    "kota": city,
                    "iqa": index,
                    "wind_dir(deg)": wind_dir,
                    "wind_spd(km/h)": wind,
                    "pressure(mbar)": pressure,
                    "accessed": time,
                }
            )

        else:
            print("Failed to retrieve the webpage. Returning nulls.")

    except:
        print("Error Occured.")


# ! FOR TESTING PURPOSES
# endpoints = endpoints[:2]

i = 0
for endpoint in endpoints:
    scrape(endpoint)
    i += 1
    print("Scraped " + str(i) + " / " + str(len(endpoints)) + " regions.")

# Write the extracted data to a CSV file
j = 0
date = datetime.now().strftime("%Y-%m-%d %H:%M")
date = datetime.strptime(date, "%Y-%m-%d %H:%M")
date = date.strftime("%Y-%m-%d-%H")
with open(f"../csv/raw-iqair/{date}.csv", mode="w") as csv_file:
    fieldnames = [
        "id",
        "kota",
        "iqa",
        "wind_dir(deg)",
        "wind_spd(km/h)",
        "pressure(mbar)",
        "accessed",
    ]
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    try:
        writer.writeheader()
        for data in dataBuffer:
            writer.writerow(data)
            j += 1
        print("Successfully wrote " + str(j) + " rows to CSV file.")
    except csv.Error as e:
        print("Error writing to CSV file.")
        print(e)
