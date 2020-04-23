import csv
import json

data = []

source = "C:\\Users\\Akshay\\Documents\\GitHub\\covid-data\\ny cases by county - ls.csv"

with open(source) as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for rows in csv_reader:
        data.append(rows)

target = "C:\\Users\\Akshay\\Documents\\GitHub\\covid-data\\data.js"

with open(target, "w") as jsonfile:
    jsonfile.write("data=")
    json.dump(data, fp=jsonfile)