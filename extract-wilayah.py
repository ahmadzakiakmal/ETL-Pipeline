import re
import json

print("=====================================")
with open("json/wilayah.json", "r") as jsonFile:
  listOfRegions = json.load(jsonFile)

with open("endpoint-wilayah.txt", "w") as outputFile:
  for region in listOfRegions:
    province = re.sub(r'(?<!^)(?=[A-Z])', '-', region["propinsi"]).lower()
    city = region["kota"].lower()
    city = city.replace("kota ", "")
    city = city.replace("kab. ", "")
    city = city.replace(" ", "-")
    
    output = f"\"/{province}/{city}\",\n"
    output = output.replace("jawa-tengah", "central-java")
    output = output.replace("jawa-barat", "west-java")
    output = output.replace("jawa-timur", "east-java")
    output = output.replace("d-k-i-jakarta", "jakarta")
    output = output.replace("d-i-yogyakarta", "yogyakarta")
    output = output.replace("kalimantan-barat", "west-kalimantan")
    output = output.replace("kalimantan-selatan", "south-kalimantan")
    output = output.replace("kalimantan-tengah", "central-kalimantan")
    output = output.replace("kalimantan-timur", "east-kalimantan")
    output = output.replace("kalimantan-utara", "north-kalimantan")
    output = output.replace("kepulauan-riau", "riau-islands")
    output = output.replace("maluku-utara", "north-maluku")
    output = output.replace("nusa-tenggara-barat", "west-nusa-tenggara")
    output = output.replace("nusa-tenggara-timur", "east-nusa-tenggara")
    output = output.replace("papua-barat", "west-papua")
    output = output.replace("sulawesi-barat", "west-sulawesi")
    output = output.replace("sulawesi-selatan", "south-sulawesi")
    output = output.replace("sulawesi-tengah", "central-sulawesi")
    output = output.replace("sulawesi-tenggara", "southeast-sulawesi")
    output = output.replace("sulawesi-utara", "north-sulawesi")
    output = output.replace("sumatera-barat", "west-sumatra")
    output = output.replace("sumatera-selatan", "south-sumatra")
    output = output.replace("sumatera-utara", "north-sumatra")
    outputFile.write(output)

  print("Output written to output.txt")  

