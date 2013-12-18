import csv
import argparse
from parser import parsers
import db
from bs4 import BeautifulSoup
from datetime import datetime


def load_xactions(**kwargs):
    reader = csv.reader(kwargs['file'], delimiter=',', quotechar='"')
    csv_parser = parsers[kwargs['parser']]
    inst = kwargs['institution']

    institution_id = db.find_institution_id(inst)
    categories_map = db.start_load()

    count = 0
    for row in reader:
        parsed_row = csv_parser.parse(row)
        count += 1
        db.insert_transaction(institution_id[0], categories_map, **parsed_row)
    print(count)


def load_qfx(institution_name, **kwargs):
    soup = BeautifulSoup(open(kwargs['file']))

    institution_id = db.find_institution_id(institution_name)
    categories_map = db.start_load()

    for tx in soup.find_all("stmttrn"):
        # some qfx files have dates in the form: 20131207000000.000[-7:MST]
        txn_date = datetime.strptime(tx.find('dtposted').contents[0].strip()[:14], "%Y%m%d%H%M%S")
        txn_type = tx.find('trntype').contents[0].strip()
        tx_amount = float(tx.find('trnamt').contents[0].strip())
        tx_description = tx.find('name').contents[0].strip()
        print("Inserted tx with " + str(tx_amount) + " on " + str(txn_date))
        db.insert_transaction(institution_id[0], categories_map, date=txn_date, amount=tx_amount,
                              description=tx_description)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='load transactions from CSV')
    parser.add_argument('-p', '--parser', default='bofa')
    parser.add_argument('-i', '--institution', default='bofa')
    parser.add_argument('file', type=file)
    args = parser.parse_args()
    load_xactions(**vars(args))

