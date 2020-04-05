import csv
import math

target = "C:\\Users\\Akshay\\Documents\\GitHub\\covid-data\\counties.csv"
source = "C:\\Users\\Akshay\\Documents\\GitHub\\covid-data\\ny cases by county.csv"
row_src = []
with open(source, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        row_src.append(row)

row_tgt = []
with open(target, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        row_tgt.append(row)

# adjust 0 values
err = 1E-2

headt = row_tgt.pop(0)
heads = row_src.pop(0)
for e in heads[1:]:
    headt.extend([e, e+"/ls"])

for rt in row_tgt:
    is_present = False
    for rs in row_src:
        if rt[1].lower() == rs[0].lower() or rt[1].lower() == rs[0].lower() + " city":
            for e in rs[1:]:
                try:
                    rt.extend([e, math.log10(int(e)+err)])
                except ValueError:
                    rt.extend([0, math.log10(0+err)])
            is_present = True
            break
    if not is_present:
        rt.extend([0, math.log10(0+err)] * len(rs[1:]))

data = [headt]
data.extend(row_tgt)

with open("C:\\Users\\Akshay\\Documents\\GitHub\\covid-data\\test.csv", 'w', newline="") as wfile:
    writer = csv.writer(wfile)
    for r in data:
        writer.writerow(r)
