from sodapy import Socrata
import datetime
import math
import json
import csv

today = datetime.date.today() -  datetime.timedelta(days=1)
today = datetime.datetime(today.year, today.month, today.day)
with open("Last updated.txt",'r') as fp:
    last_updated = fp.read()
    last_updated_obj = datetime.datetime.strptime(last_updated, '%Y-%m-%dT00:00:00.000')
if today > last_updated_obj:
    if len(check_update()) > 0:
        update_data()

app_token = "1CKHfUB8qIpEQKUM1JNdiEK1N"
socrata_dataset_identifier = "xdss-u53e"

client = Socrata("health.data.ny.gov", app_token)
metadata = client.get_metadata(socrata_dataset_identifier)

def check_update():
    results = client.get(socrata_dataset_identifier, where="test_date > '"+last_updated+"'", select="test_date, county, new_positives, cumulative_number_of_positives", limit=1)
    return results

def update_data():
    results = client.get(socrata_dataset_identifier, where="test_date >= '"+last_updated+"'", select="test_date, county, new_positives, cumulative_number_of_positives", limit=70)

    with open("dataJSON.json", 'r') as fp:
        old_data = json.load(fp)

    err = 0.01
    data = {}

    for row in results:
        date_obj = datetime.datetime.strptime(row['test_date'], '%Y-%m-%dT00:00:00.000')
        date_header = str(date_obj.month) + "/" + str(date_obj.day) + "/" + str(date_obj.year)

        entry = {'county':row['county'], 
                'newcases':int(row['new_positives']), 
                'cumcases':int(row['cumulative_number_of_positives']), 
                'ncls': int(math.log10(int(row['new_positives'])+err)),
                'ccls':int(math.log10(int(row['cumulative_number_of_positives'])+err))}
        try:
            data[date_header].append(entry)
        except KeyError:
            data[date_header] = [entry]

    for date in data:
        nyc_new_cases = 0
        nyc_cumulative_cases = 0
        for cnt in data[date]:
            if cnt['county'] == 'Bronx' or cnt['county']=='Kings' or cnt['county']=='New York' or cnt['county']=='Queens' or cnt['county']=='Richmond':
                cnt['county'] = 'New York City'
                nyc_new_cases += int(cnt['newcases'])
                nyc_cumulative_cases += int(cnt['cumcases'])
        
        for cnt in data[date]:
            if cnt['county'] == 'New York City':
                cnt['newcases'] = nyc_new_cases
                cnt['cumcases'] = nyc_cumulative_cases
                cnt['ncls'] = int(math.log10(nyc_new_cases+err))
                cnt['ccls'] = int(math.log10(nyc_cumulative_cases+err))

    row_src = []
    source = "counties.csv"
    with open(source, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            row_src.append(row)

    ids = {}
    for i in range(1, len(row_src)):
        try:
            ids[row_src[i][1]].append(row_src[i][0])
        except KeyError:
            ids[row_src[i][1]] = [row_src[i][0]]

    for date in data:
        for i in range(len(data[date])):
            cnt = data[date][i]
            entry = {'id':ids[cnt['county']].pop(0),
                    'county':cnt['county'],
                    'newcases':cnt['newcases'], 
                    'cumcases':cnt['cumcases'], 
                    'ncls': cnt['ncls'],
                    'ccls':cnt['ccls']  }
            data[date][i] = entry
            ids[cnt['county']].append( entry['id'] )

    for date in old_data:
        data[date] = old_data[date]

    target = "dataJSON.json"
    with open(target, "w") as jsonfile:
        json.dump(data, fp=jsonfile, separators=(',', ':'))

    target_js = "data.js"
    with open(target_js, "w") as jsfile:
        jsfile.write("data=")
        json.dump(data, fp=jsfile, separators=(',', ':'))
