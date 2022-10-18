import datetime
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from urllib.request import urlopen
import json
# from st_aggrid import AgGrid
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


def check_night_time(date: str):
    t = datetime.datetime.strptime(date.split('T')[1], "%H:%M:%S")
    t = t.hour
    if t == 23 or t < 6:
        return True
    else:
        return False


pon = (
    'transaction_id', 'date', 'card', 'account', 'account_valid_to', 'client', 'last_name', 'first_name',
    'patronymic',
    'date_of_birth', 'passport', 'passport_valid_to', 'phone', 'oper_type', 'amount', 'oper_result', 'terminal',
    'terminal_type', 'city', 'address')

clients = {}  # client : [rating, night, day, age, mult]
tr = []  # all transactions
patt = {}  # num_of_pattern : [all his transaction]

# with open('clients.txt', 'r') as f:
#     lines = f.readlines()
#     for line in lines:
#         l = line.replace(',', '').replace('[', '').replace(']', '').split(' ')
#         clients[l[0]] = [float(l[1]), int(l[2]), int(l[3]), int(l[4]), int(l[5])]

# with open('patt.txt', 'r') as f:
#     lines = f.readlines()
#     for line in lines:
#         l = line.replace('[', '').replace(']', '').split(' ')
#         patt[int(l[0])] = l[1:]


connection = psycopg2.connect(
    host="localhost",
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
patt[2]=[]


for k, v in clients.items():
      if v[2]/(v[1]+v[2]) > 0.55:
            v[0] -= 2.5 * v[2]
            with connection.cursor() as cursor:
                  cursor.execute("""SELECT transaction_id, date FROM transactions WHERE client=%s""",(k,))
                  temp=cursor.fetchall()
            for item in temp:
                  if check_night_time(item[1]):
                        patt[2].append(int(item[0]))

      if v[3] == 1:
            v[0] -= 1
      if v[4] == 1:
            v[0] -= 25

print(len(clients))
print(len(patt))


with open('clients.csv', 'w', newline='', encoding='utf8') as csvfile:
    fieldnames = ['client', 'rating']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for k, v in clients.items():
        writer.writerow({'client': k, 'rating': v[0]})

with open('patt.csv', 'w', newline='', encoding='utf8') as csvfile:
    fieldnames = ['id', 'opers']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'id': 1, 'opers': patt[1]})
    writer.writerow({'id': 2, 'opers': patt[2]})
    writer.writerow({'id': 3, 'opers': patt[3]})
    writer.writerow({'id': 4, 'opers': patt[4]})
    writer.writerow({'id': 5, 'opers': patt[5]})
    writer.writerow({'id': 6, 'opers': patt[6]})
    writer.writerow({'id': 7, 'opers': patt[7]})
    writer.writerow({'id': 8, 'opers': patt[8]})
    writer.writerow({'id': 9, 'opers': patt[9]})
    writer.writerow({'id': 10, 'opers': patt[10]})


with open('trans.csv', 'w', newline='', encoding='utf8') as csvfile:
    fieldnames = ['transaction_id', 'date', 'card', 'account', 'account_valid_to', 'client', 'last_name', 'first_name',
    'patronymic',
    'date_of_birth', 'passport', 'passport_valid_to', 'phone', 'oper_type', 'amount', 'oper_result', 'terminal',
    'terminal_type', 'city', 'address']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for i in range(0, len(tr)):
          writer.writerow({'transaction_id':tr[i]['transaction_id'], 'date':tr[i]['date'], 'card':tr[i]['card'], 'account':tr[i]['account'], 'account_valid_to':tr[i]['account_valid_to'], 'client':tr[i]['client'], 'last_name':tr[i]['last_name'], 'first_name':tr[i]['first_name'],
    'patronymic':tr[i]['patronymic'],
    'date_of_birth':tr[i]['date_of_birth'], 'passport':tr[i]['passport'], 'passport_valid_to':tr[i]['passport_valid_to'], 'phone':tr[i]['phone'], 'oper_type':tr[i]['oper_type'], 'amount':tr[i]['amount'], 'oper_result':tr[i]['oper_result'], 'terminal':tr[i]['terminal'],
    'terminal_type':tr[i]['terminal_type'], 'city':tr[i]['city'], 'address':tr[i]['address']})


with open('patterns.csv', 'w', newline='', encoding='utf8') as csvfile:
    fieldnames = ['id', 'name', 'description', 'sum']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'id': 1, 'sum': len(patt[1]), 'name': 'возраст','description': 'По статистике Тинькофф люди в возрасте до 33 и после 65 наиболее подвержены фроду'})
    writer.writerow({'id': 2, 'sum': len(patt[2]), 'name': 'ночное время суток','description': 'В ночное время меньше вероятность того, что владелец заметит мошеннические операции и предотвратит их '})
    writer.writerow({'id': 3, 'sum': len(patt[3]), 'name': 'перебор аккаунтов','description': 'Это может свидетельствовать о том, что злоумышленник подбирает номера карты'})
    writer.writerow({'id': 4, 'sum': len(patt[4]), 'name': 'частые операции','description': 'Такое поведение похоже на влияние программы, создающей такие запросы'})
    writer.writerow({'id': 5, 'sum': len(patt[5]), 'name': 'много отказов', 'description': 'Злоумышленник подбирает данные карты', })
    writer.writerow({'id': 6, 'sum': len(patt[6]), 'name': 'уменьшение сумм', 'description': 'Злоумышленник подбирает сумму денег, оставшуюся на карте'})
    writer.writerow({'id': 7, 'sum': len(patt[7]), 'name': 'недействительный паспорт','description': 'По закону, регистрация на недействительный паспорт невозможна '})
    writer.writerow({'id': 8, 'sum': len(patt[8]), 'name': 'разные даты валидности у паспорта','description': 'Злоумышленник пытается вмешаться в данные карты, изменение валидности паспорта'})
    writer.writerow({'id': 9, 'sum': len(patt[9]), 'name': 'адреса','description': 'Города в которых совершаются операции находятся на достаточно большом расстоянии,чтобы совершать в них операции в маленьком временном диапазоне'})
    writer.writerow({'id': 10, 'sum': len(patt[10]), 'name': 'недействительный аккаунт','description': 'Невалидные аккаунты могут быть заброшены, а значит их данные с большей долей вероятности могут быть открыты'})


with open('ages.csv', 'w', newline='', encoding='utf8') as csvfile:
    fieldnames = ['age', 'sum']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'age': '18-30', 'sum': 0})
    writer.writerow({'age': '30-40', 'sum': 1})
    writer.writerow({'age': '40-55', 'sum': 2})
    writer.writerow({'age': '55+', 'sum': 3})
    


st.title("Dashboard about anti-fraud analisys")
st.text(f"last updated: {datetime.datetime.now()}\nYou can refresh page by pressing 'R'")


st.text(f"Описание паттернов")
with open('patterns.csv', 'rb') as f:
      df = pd.read_csv(f)
st.dataframe(df)


st.text(f"Суммарно транзакций: {len(tr)}")
with open('trans.csv', 'rb') as f:
      df = pd.read_csv(f)
st.dataframe(df)


st.text(f"Суммарно клиентов: {len(clients)}")
with open('clients.csv', 'rb') as f:
      df = pd.read_csv(f)
st.dataframe(df)


st.text(f"Транзакции, подходящие под фрод паттерны")
st.text(f"Click twice on the cell to expand")
with open('patt.csv', 'rb') as f:
      df = pd.read_csv(f)
st.dataframe(df)


# st.text(f"Распределение возрастов")
# with open('ages.csv', 'rb') as f:
#       df = pd.read_csv(f)
# st.bar_chart(df)

# chart_data = pd.DataFrame(
# [10,13, 11],
# columns=["Energy Costs"])
# st.bar_chart(chart_data)