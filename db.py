import os

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, Text, ForeignKey, Date, Float
from sqlalchemy.sql import select, func, update
from datetime import date
import logging

logging.basicConfig(filename='thyme.log', level=logging.INFO)
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

if os.environ.get('HEROKU') is None:
    engine = create_engine('postgres://@localhost/thyme')
else:
    print("database URL is: " + os.environ['DATABASE_URL'])
    engine = create_engine(os.environ['DATABASE_URL'])

metadata = MetaData(bind=engine)

finins = Table('institutions', metadata,
               Column('id', Integer, primary_key=True),
               Column('name', String, unique=True),
               Column('type', String, nullable=False))

categories = Table('categories', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('parent_id', Integer, ForeignKey('categories.id')),
                   Column('name', String))

xactions = Table('transactions', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('institution_id', Integer, ForeignKey('institutions.id')),
                 Column('category_id', Integer, ForeignKey('categories.id')),
                 Column('date', Date),
                 Column('description', String),
                 Column('amount', Float))

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

for c in [UNCATEGORIZED, HOME, GROCERIES, RESTAURANTS, COFFEE, HEALTH, CASH, UTILITIES, TRAVEL, AUTO_TRANSPORT,
          PERSONAL_CARE, SHOPPING]:
    if not engine.execute(select([categories.c.id]).where(categories.c.name == c)).fetchone():
        engine.execute(categories.insert(), name=c)

category_pattern_map = {
    HOME: ['mortgage', 'hoa'],
    COFFEE: ['peets', 'starbucks', "peet's", 'coffee', 'tea', 'espresso'],
    GROCERIES: ['wholefoods', 'wholefds', 'grocery', 'safeway'],
    RESTAURANTS: ['pizza', 'pizzeria', 'deli', 'kitchen', 'restuarant'],
    AUTO_TRANSPORT: ['rotten robbie', 'chevron', 'shell', 'valero', 'caltrain', 'bart'],
    TRAVEL: ['airline', 'airlines', 'orbitz', 'kayak', 'travel'],
    PERSONAL_CARE: ['spa'],
    UTILITIES: ['vonage', 'comcast', 'vonage', 'pacific gas and'],
    CASH: ['atm'],
    HEALTH: [],
}


def exists(date, description, amount):
    stmt = select([xactions.c.id]). \
        where(xactions.c.date == date). \
        where(xactions.c.description == description). \
        where(xactions.c.amount == amount)
    return engine.execute(stmt).fetchone()


def read_for_month_year(year, month):
    start = date(year, month, 1)

    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)

    return read_txn_for_time(start, end)


def read_txn_for_time(start_time, end_time, category_name = None):
    logging.info("Reading transactions from %s to %s" % (str(start_time), str(end_time)))
    stmt = select([xactions.c.id, xactions.c.description, xactions.c.date, xactions.c.amount, categories.c.name]).\
                    where(xactions.c.date >= start_time).\
                    where( xactions.c.date < end_time).\
                    select_from(xactions.join(categories))
    if category_name:
        stmt = stmt.where(categories.c.name == category_name)

    stmt = stmt.order_by(xactions.c.date)

    return engine.execute(stmt)

def read_txn_for_time_by_category(start_time, end_time):
    stmt = select([categories.c.name, func.sum(xactions.c.amount)]).\
                    where(xactions.c.date >= start_time).\
                    where( xactions.c.date < end_time).\
                    select_from(xactions.join(categories)).\
                    group_by(categories.c.name)\


    return engine.execute(stmt)


def find_institution_id(name):
    stmt = select([finins.c.id]).where(finins.c.name == name)
    return engine.execute(stmt).fetchone()


def find_category_id(name):
    stmt = select([categories.c.id]).where(categories.c.name == name)
    return engine.execute(stmt).fetchone()[0]


def start_load():
    rows = engine.execute(select([categories]))
    category_map = {}
    for r in rows:
        category_map[r['name']] = r['id']
    return category_map


def insert_transaction(institution_id, categories_map, **kwargs):
    if len(kwargs) != 3:
        return

    dt = kwargs['date']
    description = kwargs['description']
    amount = kwargs['amount']

    skip = exists(dt, description, amount)

    if not skip:
        engine.execute(xactions.insert(),
                       institution_id=institution_id,
                       category_id=guess_category(description, categories_map),
                       date=dt,
                       description=description,
                       amount=amount)


def guess_category(description, categories_map):
    for category_name in category_pattern_map:
        possible_patterns = category_pattern_map[category_name]
        for pattern in possible_patterns:
            # Use regexp (\bpattern\b) instead of just string contains.
            if pattern in description.lower():
                return categories_map[category_name]

    return categories_map[UNCATEGORIZED]


def list_categories():
    return engine.execute(select([categories]))


def list_institutions():
    return engine.execute(select([finins]))


def update_tx_category(txid, category_id):
    stmt = update(xactions).where(xactions.c.id == txid).values(category_id = category_id)
    upd = engine.execute(stmt)
    return upd.rowcount


def create_institution(name, type):
    print("called create institution")
    engine.execute(finins.insert(), name=name, type=type)