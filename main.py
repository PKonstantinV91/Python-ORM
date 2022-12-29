import json
import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import create_tables, Book, Publisher, Shop, Stock, Sale


login = input("Введите логин: ")
password = input("Пароль: ")
name_base = input("Название БД: ")

DSN = f'postgresql://{login}:{password}@localhost:5432/{name_base}'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

current = os.getcwd()
file_name = input("Название файла:")
full_path = os.path.join(current, file_name)
with open(full_path, 'r') as td:
    data = json.load(td)
for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()

name = input("Введите имя автора: ")

result = session.query(Stock, Book.title, Shop.name, Sale.price, Sale.date_sale)
result = result.join(Sale)
result = result.join(Shop)
result = result.join(Book)
result = result.join(Publisher)
result = result.filter(Publisher.name == name)
for c in result:
    print(*c[1:], sep="|")
session.close()
