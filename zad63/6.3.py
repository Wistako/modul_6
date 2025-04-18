import csv
from sqlalchemy import create_engine, text
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, Date
from datetime import datetime

engine = create_engine('sqlite:///zad63/database.db', echo=True)
conn = engine.connect()

meta = MetaData()

# Definicja tabeli stations
stations = Table(
   'stations', meta,
   Column('id', Integer, primary_key=True, autoincrement=True),
   Column('station', String),
   Column('latitude', Float),
   Column('longitude', Float),
   Column('elevation', Float),
   Column('name', String),
   Column('country', String),
   Column('state', String),
)

measurements = Table(
   'measurements', meta,
   Column('id', Integer, primary_key=True, autoincrement=True),
   Column('station', String),
   Column('date', Date),
   Column('precip', Float),
   Column('tobs', Float),
)

meta.create_all(engine)

with open('zad63/clean_stations.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        ins = stations.insert().values(
            station=row['station'],
            latitude=float(row['latitude']),
            longitude=float(row['longitude']),
            elevation=float(row['elevation']),
            name=row['name'],
            country=row['country'],
            state=row['state']
        )
        conn.execute(ins)

with open('zad63/clean_measure.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        ins = measurements.insert().values(
            station=row['station'],
            date=datetime.strptime(row['date'], '%Y-%m-%d').date(),
            precip=float(row['precip']) if row['precip'] else None,
            tobs=float(row['tobs']) if row['tobs'] else None
        )
        conn.execute(ins)

result = conn.execute(text("SELECT * FROM stations LIMIT 5")).fetchall()
print("Przykładowe dane ze stacji:")
for row in result:
    print(row)

conn.close()









