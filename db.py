import os

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, Text, ForeignKey, Date, Float
from sqlalchemy.sql import select, func, update
from datetime import date
from sqlite3 import dbapi2 as sqlite

import logging

logging.basicConfig(filename='thyme.log', level=logging.INFO)
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

if os.environ.get('HEROKU') is None:
#    engine = create_engine('postgres://@localhost/thyme')
    engine = create_engine('sqlite+pysqlite:///thyme.db', module=sqlite)
else:
    print("database URL is: " + os.environ['DATABASE_URL'])
    engine = create_engine(os.environ['DATABASE_URL'])

metadata = MetaData(bind=engine)

finins = Table('accounts', metadata,
               Column('id', Integer, primary_key=True),
               Column('nickname', String, unique=True),
               Column('fid', Integer),
               Column('name', String),
               Column('type', String))

categories = Table('categories', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('parent_id', Integer, ForeignKey('categories.id')),
                   Column('name', String),
                   Column('budget', Integer, default=0))

xactions = Table('transactions', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('institution_id', Integer, ForeignKey('accounts.id')),
                 Column('category_id', Integer, ForeignKey('categories.id')),
                 Column('date', Date),
                 Column('fitid', String),
                 Column('description', String),
                 Column('amount', Float))

description_category_mapping = Table('desc_category_mapping', metadata,
                                     Column('id', Integer, primary_key=True),
                                     Column('description', String),
                                     Column('category_id', Integer, ForeignKey('categories.id')))

files_loaded = Table('files', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('name', String),
                     Column('mtime', Integer),
                     Column('md5', String))

metadata.create_all(engine)

UNCATEGORIZED = 'uncategorized'
HOME = 'home'
UTILITIES = 'utilities'
GROCERIES = 'groceries'
RESTAURANTS = 'restaurants'
ENTERTAINMENT = 'entertainment'
COFFEE = 'coffee'
HEALTH = 'health'
CASH = 'cash/atm'
GIFT = 'gift/donations'
AUTO_TRANSPORT = 'auto/transportation'
INCOME = 'income'
TRAVEL = 'travel'
TAXES = 'taxes'
FEES = 'fees/interest'
PERSONAL_CARE = 'personal_care'
SHOPPING = 'shopping'
TRANSFER = 'transfer'
PAYCHECK = 'paycheck'

category_pattern_map = {
    HOME: ['mortgage', 'hoa'],
    COFFEE: ['peets', 'starbucks', "peet's", 'coffee', 'tea', 'espresso'],
    GROCERIES: ['wholefoods', 'wholefds', 'grocery', 'safeway'],
    RESTAURANTS: ['pizza', 'pizzeria', 'deli', 'kitchen', 'restuarant', 'bistro'],
    AUTO_TRANSPORT: ['rotten robbie', 'chevron', 'shell', 'valero', 'caltrain', 'bart'],
    TRAVEL: ['airline', 'airlines', 'orbitz', 'kayak', 'travel'],
    PERSONAL_CARE: ['spa'],
    UTILITIES: ['vonage', 'comcast', 'vonage', 'pacific gas and', 'at&t'],
    CASH: ['atm'],
    HEALTH: [],
    TRANSFER: [],
    UNCATEGORIZED: [],
    PAYCHECK: [],

    ENTERTAINMENT: ['netflix', 'amc', 'theater', 'theatre']
}

for c in category_pattern_map.keys():
    if not engine.execute(select([categories.c.id]).where(categories.c.name == c)).fetchone():
        engine.execute(categories.insert(), name=c)


def exists(fitid):
    stmt = select([xactions.c.id]).where(xactions.c.fitid == fitid)
    fv = engine.execute(stmt).fetchone()
    return fv


def read_for_month_year(year, month):
    start = date(year, month, 1)

    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)

    return read_txn_for_time(start, end)


def read_txn_for_time(start_time, end_time, category_name=None):
    logging.info("Reading transactions from %s to %s" % (str(start_time), str(end_time)))
    stmt = select(
        [xactions.c.id, xactions.c.description, xactions.c.date, xactions.c.amount, categories.c.name, finins.c.nickname]).\
        where(xactions.c.date >= start_time). \
        where(xactions.c.date < end_time). \
        select_from(xactions.join(categories).join(finins))
    if category_name:
        stmt = stmt.where(categories.c.name == category_name)

    stmt = stmt.order_by(xactions.c.date)

    return engine.execute(stmt)


def read_txn_for_time_by_category(start_time, end_time):
    stmt = select([categories.c.name, func.sum(xactions.c.amount)]). \
        where(xactions.c.date >= start_time). \
        where(xactions.c.date < end_time). \
        select_from(xactions.join(categories)). \
        group_by(categories.c.name)\


    return engine.execute(stmt)


def find_institution_id(name, fid):
    stmt = select([finins.c.id]).where(finins.c.name == name.strip()).where(finins.c.fid == int(fid))
    account_id = engine.execute(stmt).fetchone()

    if not account_id:
        res = engine.execute(finins.insert(), name=name.strip(), fid=fid)
        return res.inserted_primary_key[0]
    else:
        return account_id[0]


def find_category_id(name):
    stmt = select([categories.c.id]).where(categories.c.name == name)
    return engine.execute(stmt).fetchone()[0]


def load_categories():
    rows = engine.execute(select([categories]))
    category_map = {}
    for r in rows:
        category_map[r['name']] = r['id']
    return category_map

def load_desc_category():
    rows = engine.execute(select([description_category_mapping]))
    desc_category_map = {}
    for r in rows:
        desc_category_map[r['description']] = r['category_id']
    return desc_category_map


def insert_transaction(institution_id, categories_map, desc_category_map, **kwargs):
    if len(kwargs) != 4:
        return

    skip = exists(kwargs['fitid'])

    if skip:
        return False

    dt = kwargs['date']
    description = kwargs['description']
    amount = kwargs['amount']

    engine.execute(xactions.insert(),
                   institution_id=institution_id,
                   category_id=guess_category(description, categories_map, desc_category_map),
                   date=dt,
                   description=description,
                   amount=amount,
                   fitid=kwargs['fitid'])
    return True


def guess_category(description, categories_map, desc_category_mapping):
    cleaned_description = clean_description(description)
    if cleaned_description in desc_category_mapping:
        return desc_category_mapping[cleaned_description]

    for category_name in category_pattern_map:
        possible_patterns = category_pattern_map[category_name]
        for pattern in possible_patterns:
            # Use regexp (\bpattern\b) instead of just string contains.
            if pattern in cleaned_description:
                return categories_map[category_name]

    return categories_map[UNCATEGORIZED]


def list_categories():
    return engine.execute(select([categories]))


def list_institutions():
    return engine.execute(select([finins.c.id, finins.c.nickname, finins.c.name, finins.c.fid]))


def update_tx_category(txid, category_id):
    stmt = update(xactions).where(xactions.c.id == txid).values(category_id=category_id)
    upd = engine.execute(stmt)

    if upd.rowcount == 1:
        stmt = select([xactions.c.description]).where(xactions.c.id == txid)
        desc = engine.execute(stmt).fetchone()
        update_description_mapping(desc[0], category_id)
    return upd.rowcount


def update_description_mapping(desc, category_id):
    cleaned_desc = desc.strip().lower()
    stmt = select([description_category_mapping.c.id]).where(description_category_mapping.c.description == cleaned_desc)

    if not engine.execute(stmt).fetchone():
        engine.execute(description_category_mapping.insert(), description=cleaned_desc, category_id=category_id)
    else:
        stmt = update(description_category_mapping).where(description_category_mapping.c.description == cleaned_desc).values(category_id=category_id)
        engine.execute(stmt)

def clean_description(desc):
    return desc.strip().lower()

def create_institution(name, type):
    print("called create institution")
    engine.execute(finins.insert(), name=name, type=type)


def need_to_load(file, stat):
    if "qfx" not in file.lower():
        return False

    stmt = select([files_loaded])
    loaded_files = engine.execute(stmt)

    for loaded_file in loaded_files:
        if loaded_file['name'] == file and int(loaded_file['mtime']) == int(stat.st_mtime):
            return False

    return True


def file_loaded(file, stat):
    engine.execute(files_loaded.insert(), name=file, mtime=int(stat.st_mtime))


def update_institution(id, nickname):
    stmt = update(finins).where(finins.c.id == id).values(nickname=nickname)
    engine.execute(stmt)
