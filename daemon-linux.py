from sodapy import Socrata
import datetime
import math
import json
import csv
import time
import os

def check_update():
    results = client.get(socrata_dataset_identifier, where="test_date > '"+last_updated+"'", select="test_date, county, new_positives, cumulative_number_of_positives", limit=1)
    return results

def update_data():
    results = client.get(socrata_dataset_identifier, where="test_date > '"+last_updated+"'", select="test_date, county, new_positives, cumulative_number_of_positives", limit=1000)

    row_src = []
    source = "ny cases by county - ls.csv"
    with open(source, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        headers = next(csvreader)
    with open(source, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            for key in headers:
                if "ls" in key:
                    row.pop(key)
            row_src.append(row)

    for row in results:
        date_obj = datetime.datetime.strptime(row['test_date'], '%Y-%m-%dT00:00:00.000')
        date_header = str(date_obj.month) + "/" + str(date_obj.day) + "/" + str(date_obj.year)
        for entry in row_src: 
            if entry['county'].lower() == row['county'].lower():
                entry[date_header] = row['cumulative_number_of_positives']
    

    target = source
    with open(target, "w", newline="") as csvfile:
        headers = list(row_src[0].keys())
        writer = csv.DictWriter(csvfile, headers)
        writer.writeheader()
        for row in row_src:
            writer.writerow(row)

def runTask():

    today = datetime.date.today() -  datetime.timedelta(days=1)
    today = datetime.datetime(today.year, today.month, today.day)

    if today > last_updated_obj:
        if len(check_update()) > 0:
            update_data()
            print("Data files updated")

            with open("Last updated.txt", 'w') as fp:
                fp.write(today.strftime('%Y-%m-%dT00:00:00.000'))

# def createDaemon():
#   try:
#     # Store the Fork PID
#     pid = os.fork()
#     if pid > 0:
#       print('PID: ',pid)
#       os._exit(0)
#   except error:
#     print('Unable to fork. Error: ', error.errno, error.strerror)
#     os._exit(1)

#   runTask()

if __name__ == '__main__':
    # API config (Do not change)
    app_token = "1CKHfUB8qIpEQKUM1JNdiEK1N"
    socrata_dataset_identifier = "xdss-u53e"

    client = Socrata("health.data.ny.gov", app_token)
    metadata = client.get_metadata(socrata_dataset_identifier)

    with open("Last updated.txt",'r') as fp:
        last_updated = fp.read()
        last_updated_obj = datetime.datetime.strptime(last_updated, '%Y-%m-%dT00:00:00.000')

    while True:
        runTask()
        with open("logs.txt", "a+") as fp:
            fp.write("Last run at "+ str(time.ctime())+"\n")
        time.sleep(600)