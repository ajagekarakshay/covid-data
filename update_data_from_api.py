from sodapy import Socrata
import datetime
import math
import json
import csv



def check_update():
    results = client.get(socrata_dataset_identifier, where="test_date > '"+last_updated+"'", select="test_date, county, new_positives, cumulative_number_of_positives", limit=1)
    print("Checking update.....")
    return results

def update_data():
    results = client.get(socrata_dataset_identifier, where="test_date > '"+last_updated+"'", select="test_date, county, new_positives, cumulative_number_of_positives", limit=70)

    with open("dataJSON.json", 'r') as fp:
        old_data = json.load(fp)

    err = 0.01
    data = {}

    for row in results:
        date_obj = datetime.datetime.strptime(row['test_date'], '%Y-%m-%dT00:00:00.000')
        date_header = str(date_obj.month) + "/" + str(date_obj.day) + "/" + str(date_obj.year)

        entry = {'county':row['county'], 
                'cumcases':int(row['cumulative_number_of_positives'])}
        try:
            data[date_header].append(entry)
        except KeyError:
            data[date_header] = [entry]

    row_src = []
    source = "ny cases by county - ls.csv"
    with open(source, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            row_src.append(row)
            for key in row_src[-1]:
                if "ls" in key:
                    row_src[-1].pop(key)


    for date in old_data:
        data[date] = old_data[date]

    target = "dataJSON.json"
    with open(target, "w") as jsonfile:
        json.dump(data, fp=jsonfile, separators=(',', ':'))


app_token = "1CKHfUB8qIpEQKUM1JNdiEK1N"
socrata_dataset_identifier = "xdss-u53e"

client = Socrata("health.data.ny.gov", app_token)
metadata = client.get_metadata(socrata_dataset_identifier)


# today = datetime.date.today() -  datetime.timedelta(days=1)
# today = datetime.datetime(today.year, today.month, today.day)
# with open("Last updated.txt",'r') as fp:
#     last_updated = fp.read()
#     last_updated_obj = datetime.datetime.strptime(last_updated, '%Y-%m-%dT00:00:00.000')

# if today > last_updated_obj:
#     if len(check_update()) > 0:
#         update_data()
#         print("Data files updated")

#         with open("Last updated.txt", 'w') as fp:
#             fp.write(today.strftime('%Y-%m-%dT00:00:00.000'))

