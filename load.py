
import csv
import argparse
from parser import parsers
import db

def main(**kwargs):
    reader = csv.reader(kwargs['file'], delimiter=',', quotechar='"')
    csv_parser = parsers[kwargs['parser']]
    inst = kwargs['institution']

    institution_id = db.find_institution_id(inst)
    categories_map = db.start_load()

    for row in reader:
        parsed_row = csv_parser.parse(row)
        db.insert_transaction(institution_id[0], categories_map, **parsed_row)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='load transactions from CSV')
    parser.add_argument('-p', '--parser', default='bofa')
    parser.add_argument('-i', '--institution', default='bofa')
    parser.add_argument('file', type=file)
    args = parser.parse_args()
    main(**vars(args))

