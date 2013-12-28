import csv
import argparse
from parser import parsers
import db
import os
from os.path import expanduser
from bs4 import BeautifulSoup
from datetime import datetime


def load_xactions(**kwargs):
    reader = csv.reader(kwargs['file'], delimiter=',', quotechar='"')
    csv_parser = parsers[kwargs['parser']]
    inst = kwargs['institution']

    institution_id = db.find_institution_id(inst)
    categories_map = db.load_categories()

    count = 0
    for row in reader:
        parsed_row = csv_parser.parse(row)
        count += 1
        db.insert_transaction(institution_id[0], categories_map, **parsed_row)
    print(count)


def load_qfx(institution_name, **kwargs):
    soup = BeautifulSoup(open(kwargs['file']))

    if institution_name:
        institution_id = db.find_institution_id(institution_name)
    else:
        institution_id = db.find_institution_id(soup.find("org").contents[0], soup.find("org").find("fid").contents[0])

    categories_map = db.load_categories()
    desc_category_map = db.load_desc_category()

    total_inserted = 0
    total = 0
    for tx in soup.find_all("stmttrn"):
        total += 1
        # some qfx files have dates in the form: 20131207000000.000[-7:MST]
        txn_date = datetime.strptime(tx.find('dtposted').contents[0].strip()[:14], "%Y%m%d%H%M%S")
        tx_amount = float(tx.find('trnamt').contents[0].strip())
        tx_description = tx.find('name').contents[0].strip()
        tx_fitid = tx.find('fitid').contents[0].strip()
        total_inserted += db.insert_transaction(institution_id[0], categories_map, desc_category_map, date=txn_date, amount=tx_amount,
                              description=tx_description, fitid=tx_fitid)

    db.file_loaded(kwargs['file'], os.stat(kwargs['file']))

    print(str(total_inserted) + "/" + str(total) + " transactions inserted for " + kwargs['file'])


def load_qfx_new(dir=None):
    if not dir:
        dir = expanduser("~") + "/Downloads"

    files = []
    for file in os.listdir(dir):
        fullpath = dir + "/" + file
        if db.need_to_load(fullpath, os.stat(fullpath)):
            files.append(fullpath)

    for f in files:
        load_qfx(None, file=f)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='load transactions from CSV')
    parser.add_argument('-p', '--parser', default='bofa')
    parser.add_argument('-i', '--institution', default='bofa')
    parser.add_argument('file', type=file)
    args = parser.parse_args()
    load_xactions(**vars(args))

