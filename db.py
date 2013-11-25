import os

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, Text, ForeignKey, Date, Float
from sqlalchemy.sql import select
from datetime import date

if (os.environ.get('HEROKU') is None):
    engine = create_engine('postgres://@localhost/thyme', echo=True)
else:
    print("database URL is: " + os.environ['DATABASE_URL'])
    engine = create_engine(os.environ['DATABASE_URL'])

metadata = MetaData(bind=engine)

finins = Table('institutions', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String))

xactions = Table('transactions', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('institution_id', Integer, ForeignKey('institutions.id')),
                 Column('date', Date),
                 Column('description', String),
                 Column('amount', Float))

metadata.create_all(engine)

def exists(date, description, amount):
    stmt = select([xactions.c.id]).\
              where(xactions.c.date == date).\
              where(xactions.c.description == description).\
              where(xactions.c.amount == amount)
    return engine.execute(stmt).fetchone()

def insert_xaction(institution_id, row):
    dt_fields = [int(r) for r in row[0].split("/")]
    dt = date(dt_fields[2], dt_fields[0], dt_fields[1])
    print(dt)
    description = row[1]
    amount = float(row[2])

    skip = exists(dt, description, amount)
    if not skip:
        engine.execute(xactions.insert(),
                       institution_id = institution_id,
                       date = dt,
                       description = description,
                       amount = amount)

insert_xaction(1, ['10/31/2013',"TRINET DES:PAYROLL ID:00001055623 INDN:KHETTRY,MANISH CO ID:19433...","0.07","4515.26"])

#print(exists(date(2013, 10, 31), 'TRINET DES:PAYROLL ID:00001055623 INDN:KHETTRY,MANISH CO ID:19433...', 0.07))