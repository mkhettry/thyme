import db

import csv
with open('stmt.csv', 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        if len(row) != 4:
            continue
        if row[0] == "Date":
            continue
        if not row[2]:
            continue
        db.insert_xaction(1, row)


