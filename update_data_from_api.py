from sodapy import Socrata
import datetime
import math
import json
import csv

# Population
ny_population = {'Albany': 303833,
 'Allegany': 48943,
 'Bronx': 1386657,
 'Broome': 200272,
 'Cattaraugus': 80229,
 'Cayuga': 79978,
 'Chautauqua': 134768,
 'Chemung': 88824,
 'Chenango': 50405,
 'Clinton': 82115,
 'Columbia': 63073,
 'Cortland': 49294,
 'Delaware': 47832,
 'Dutchess': 297577,
 'Erie': 918652,
 'Essex': 39302,
 'Franklin': 51579,
 'Fulton': 55526,
 'Genesee': 60082,
 'Greene': 49159,
 'Hamilton': 4831,
 'Herkimer': 64502,
 'Jefferson': 116582,
 'Kings': 2508340,
 'Lewis': 27072,
 'Livingston': 65333,
 'Madison': 73439,
 'Monroe': 744389,
 'Montgomery': 50250,
 'Nassau': 1341245,
 'New york': 1586698,
 'Niagara': 216546,
 'Oneida': 234845,
 'Onondaga': 467204,
 'Ontario': 108116,
 'Orange': 373355,
 'Orleans': 42854,
 'Oswego': 122114,
 'Otsego': 62232,
 'Putnam': 99713,
 'Queens': 2233841,
 'Rensselaer': 159428,
 'Richmond': 469363,
 'Rockland': 312183,
 'Saratoga': 219960,
 'Schenectady': 154889,
 'Schoharie': 32722,
 'Schuyler': 18330,
 'Seneca': 35199,
 'St. Lawrence': 111894,
 'Steuben': 98930,
 'Suffolk': 1494434,
 'Sullivan': 77500,
 'Tioga': 51049,
 'Tompkins': 101620,
 'Ulster': 182435,
 'Warren': 65697,
 'Washington': 63288,
 'Wayne': 93753,
 'Westchester': 950541,
 'Wyoming': 42129,
 'Yates': 25338}

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
                'newcases':int(row['new_positives']), 
                'cumcases':int(row['cumulative_number_of_positives']), 
                'ncls': int(math.log10(int(row['new_positives'])+err)),
                'ccls':int(math.log10(int(row['cumulative_number_of_positives'])+err))}
        try:
            data[date_header].append(entry)
        except KeyError:
            data[date_header] = [entry]

    # for date in data:
    #     nyc_new_cases = 0
    #     nyc_cumulative_cases = 0
    #     for cnt in data[date]:
    #         if cnt['county'] == 'Bronx' or cnt['county']=='Kings' or cnt['county']=='New York' or cnt['county']=='Queens' or cnt['county']=='Richmond':
    #             cnt['county'] = 'New York City'
    #             nyc_new_cases += int(cnt['newcases'])
    #             nyc_cumulative_cases += int(cnt['cumcases'])
        
    #     for cnt in data[date]:
    #         if cnt['county'] == 'New York City':
    #             cnt['newcases'] = nyc_new_cases
    #             cnt['cumcases'] = nyc_cumulative_cases
    #             cnt['ncls'] = int(math.log10(nyc_new_cases+err))
    #             cnt['ccls'] = int(math.log10(nyc_cumulative_cases+err))

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
        jsfile.write("\n"+"ny_pop=")
        json.dump(ny_population, fp=jsfile, separators=(',', ':'))


app_token = "1CKHfUB8qIpEQKUM1JNdiEK1N"
socrata_dataset_identifier = "xdss-u53e"

client = Socrata("health.data.ny.gov", app_token)
metadata = client.get_metadata(socrata_dataset_identifier)


today = datetime.date.today() -  datetime.timedelta(days=1)
today = datetime.datetime(today.year, today.month, today.day)
with open("Last updated.txt",'r') as fp:
    last_updated = fp.read()
    last_updated_obj = datetime.datetime.strptime(last_updated, '%Y-%m-%dT00:00:00.000')

if today > last_updated_obj:
    if len(check_update()) > 0:
        update_data()
        print("Data files updated")

        with open("Last updated.txt", 'w') as fp:
            fp.write(today.strftime('%Y-%m-%dT00:00:00.000'))

