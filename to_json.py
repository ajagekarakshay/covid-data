import csv
import json
import numpy as np
data = []

source = "C:\\Users\\Akshay\\Documents\\GitHub\\covid-data\\ny cases by county - ls.csv"

with open(source) as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for rows in csv_reader:
        data.append(rows)

for e in data:
    for key in e:
        try:
            e[key] = int(e[key])
        except:
            try:
                e[key] = np.round(float(e[key]), 2)
            except:
                pass

target = "C:\\Users\\Akshay\\Documents\\GitHub\\covid-data\\data.js"

with open(target, "w") as jsonfile:
    jsonfile.write("jsData=")
    json.dump(data, fp=jsonfile, separators=(',', ':'))