import datetime
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from urllib.request import urlopen
import json
from st_aggrid import AgGrid
import csv
import psycopg2
frod_types = {'to_old_or_young': 1,
              'same_card_num': 3,
              'fast_operations': 4,
              'many_declines': 5,
              'decreasing_operation_sum': 6,
              'invalid_passport': 7,
              'account_validation': 10,
              'dist': 9,
              'multiple_passport_validation': 8}

pon = (
    'transaction_id', 'date', 'card', 'account', 'account_valid_to', 'client', 'last_name', 'first_name',
    'patronymic',
    'date_of_birth', 'passport', 'passport_valid_to', 'phone', 'oper_type', 'amount', 'oper_result', 'terminal',
    'terminal_type', 'city', 'address')

with open('patterns.csv', 'w', newline='', encoding='utf8') as csvfile:
    fieldnames = ['id', 'name', 'description']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'id': 1, 'name': 'возраст',
                     'description': 'По статистике Тинькофф люди в возрасте до 33 и после 65 наиболее подвержены фроду'})
    writer.writerow({'id': 2, 'name': 'ночное время суток',
                     'description': 'В ночное время меньше вероятность того, что владелец заметит мошеннические операции и предотвратит их '})
    writer.writerow({'id': 3, 'name': 'перебор аккаунтов',
                     'description': 'Это может свидетельствовать о том, что злоумышленник подбирает номера карты'})
    writer.writerow({'id': 4, 'name': 'частые операции',
                     'description': 'Такое поведение похоже на влияние программы, создающей такие запросы'})
    writer.writerow({'id': 5, 'name': 'много отказов', 'description': 'Злоумышленник подбирает данные карты'})
    writer.writerow(
        {'id': 6, 'name': 'уменьшение сумм', 'description': 'Злоумышленник подбирает сумму денег, оставшуюся на карте'})
    writer.writerow({'id': 7, 'name': 'недействительный паспорт',
                     'description': 'По закону, регистрация на недействительный паспорт невозможна '})
    writer.writerow({'id': 8, 'name': 'разные даты валидности у паспорта',
                     'description': 'Злоумышленник пытается вмешаться в данные карты, изменение валидности паспорта'})
    writer.writerow({'id': 9, 'name': 'адреса',
                     'description': 'Города в которых совершаются операции находятся на достаточно большом расстоянии,чтобы совершать в них операции в маленьком временном диапазоне'})
    writer.writerow({'id': 10, 'name': 'недействительный аккаунт',
                     'description': 'Невалидные аккаунты могут быть заброшены, а значит их данные с большей долей вероятности могут быть открыты'})

clients = {}  # client : [rating, night, day, age, mult]
tr = []  # all transactions
patt = {}  # num_of_pattern : [all his transaction]

connection = psycopg2.connect(
    host="db_auth",
    user="admin",
    password="root",
    database="postgres",
    port=5432
)
with connection.cursor() as cursor:
    cursor.execute("""SELECT * FROM client_rating""")
    temp = cursor.fetchall()
for i in temp:
    clients[i[0]] = []
    for j in range(1, len(i)):
        clients[i[0]].append(i[j])

with connection.cursor() as cursor:
    cursor.execute("""SELECT * FROM transactions""")
    temp = cursor.fetchall()
for i in temp:
    joke = {}
    for j in range(len(i)):
        joke[pon[j]] = i[j]
    tr.append(joke)

for i in frod_types:
    with connection.cursor() as cursor:
        cursor.execute("""SELECT * FROM {tab} ;""".format(tab=i))
        temp = cursor.fetchall()
    joke=[]
    for j in temp:
        joke.append(j[0])
    patt[frod_types[i]] = joke

# with open('patterns.csv', 'rb') as f:
#       df = pd.read_csv(f)
# # print(type(df))
# st.title("Dashboard about anti-fraud analisys")
# st.text(f"last updated: {datetime.datetime.now()}\nYou can refresh page by pressing 'R'")
# st.text(f"Описание паттернов")
# AgGrid(df, columns_auto_size_mode=True)
# st.text(f"Суммарно транзакций: {0}")
# with open('clients.csv', 'rb') as f:
#       df = pd.read_csv(f)
# st.text(f"Суммарно клиентов: {0}")

print(len(clients))
print(len(patt))

with open('clients.txt', 'w') as f:
    for k, v in clients.items():
        f.write(str(k) + ' ' + str(v)+'\n')

with open('patt.txt', 'w') as f:
    for k, v in patt.items():
        f.write(str(k) + ' ' + str(v)+'\n')