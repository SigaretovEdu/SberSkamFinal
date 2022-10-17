from flask import Flask, request
import json
import psycopg2
import time
import wikipedia
from os.path import exists
from geopy.distance import geodesic
import datetime


frod_types=['to_old_or_young',
'night_time',
'same_card_num',
'fast_operations',
'many_declines',
'decreasing_operation_sum',
'invalid_password',
'interrupt_in_card_values',
'noname']

rating ="""CREATE TABLE IF NOT EXISTS client_rating (
    client_id CHARACTER VARYING (10),
    rating REAL,
    day_operations INT,
    night_operations INT
);"""

validat="""CREATE TABLE IF NOT EXISTS validation (
    passport CHARACTER VARYING (25),
    passport_valid_to CHARACTER VARYING (25)
)"""

initcommand = """CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    date CHARACTER VARYING (25),
    card CHARACTER VARYING (25),
    account CHARACTER VARYING (25),
    account_valid_to CHARACTER VARYING (25),
    client CHARACTER VARYING (10),
    last_name CHARACTER VARYING (25),
    first_name CHARACTER VARYING (25),
    patronymic CHARACTER VARYING (25),
    date_of_birth CHARACTER VARYING (25),
    passport CHARACTER VARYING (25),
    passport_valid_to CHARACTER VARYING (25),
    phone CHARACTER VARYING (15),
    oper_type CHARACTER VARYING (15),
    amount REAL,
    oper_result CHARACTER VARYING (15),
    terminal CHARACTER VARYING (15),
    terminal_type CHARACTER VARYING (10),
    city CHARACTER VARYING (25),
    address CHARACTER VARYING (100)
);"""

select_all = """ 
    SELECT * FROM transactions WHERE client=%s 
"""

select_last_n = """ 
    SELECT * FROM transactions WHERE client=%s ORDER BY transaction_id DESC LIMIT %s  
"""


pon = (
    'transaction_id', 'date', 'card', 'account', 'account_valid_to', 'client', 'last_name', 'first_name',
    'patronymic',
    'date_of_birth', 'passport', 'passport_valid_to', 'phone', 'oper_type', 'amount', 'oper_result', 'terminal',
    'terminal_type', 'city', 'address')


add_command = """
    INSERT INTO transactions (date,card,account,account_valid_to,client,last_name,first_name,patronymic,date_of_birth,passport,passport_valid_to,phone,oper_type,amount,oper_result,terminal,terminal_type,city,address) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

app = Flask(__name__)


def init(data: dict):
    l = []
    for item in data:
        l.append(data[item])
    a = tuple(l)
    return a


@app.route('/', methods=['POST', 'GET'])
def mail():
    request_data = request.json
    bill = 0
    if check_age(request_data):
        bill += 0.5




    with connection.cursor() as cursor:
        cursor.execute(add_command, init(request_data))
    connection.commit()
    return request_data


if __name__ == "__main__":
    time.sleep(5)
    connection = psycopg2.connect(
        host="db_auth",
        user="admin",
        password="root",
        database="postgres",
        port=5432
    )
    with connection.cursor() as cursor:
        cursor.execute(initcommand)
        cursor.execute(rating)
        cursor.execute(validat)
        for i in frod_types:
            cursor.execute("""CREATE TABLE IF NOT EXISTS {tab} (
                transaction_id SERIAL PRIMARY KEY
                )""".format(tab=i))
    connection.commit()
    app.run(host="0.0.0.0", port=7100, debug=True)


def check_diff(s1: str, s2: str):
    f = False
    for i in range(0, len(s1)):
        if s1[i] != s2[i]:
            if not f:
                f = True
            else:
                return False
    if f:
        return True
    else:
        return False


def check_failed_passport_validation(data: dict, client_rating: dict, f=0):
    print('*****\ndetected for failed passport validation')
    answer = []
    for item in data:
        if datetime.datetime.strptime(data[item]['date'].split('T')[0], "%Y-%m-%d") > datetime.datetime.strptime(
                data[item]['passport_valid_to'].split('T')[0], "%Y-%m-%d"):
            client_rating[data[item]['client']] -= 16
            answer.append(item)
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_multiple_passport_validation(data: dict, client_rating: dict, f=0):
    print('*****\ndetected for multiple passport validation')
    answer = []
    d = {}
    for item in data:
        if (data[item]['client'], data[item]['passport']) not in d.keys():
            d[(data[item]['client'], data[item]['passport'])] = {data[item]['passport_valid_to']}
        else:
            d[(data[item]['client'], data[item]['passport'])].add((data[item]['passport_valid_to']))
    for k, v in d.items():
        if len(v) > 1:
            client_rating[k[0]] -= 17
            for item in data:
                if data[item]['client'] == k[0] and data[item]['passport'] == k[1]:
                    answer.append(item)
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_failed_account_validation(data: dict, client_rating: dict, f=0):
    print('*****\ndetected for failed account validation')
    answer = []
    for item in data:
        if datetime.datetime.strptime(data[item]['date'].split('T')[0], "%Y-%m-%d") > datetime.datetime.strptime(
                data[item]['account_valid_to'].split('T')[0], "%Y-%m-%d"):
            answer.append((item))
            client_rating[data[item]['client']] -= 2
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_brute(data: dict, client_rating: dict, f=0):
    print('*****\ndetected for brute')
    d = {}
    for item in data:
        if data[item]['client'] not in d.keys():
            d[data[item]['client']] = [(item, data[item]['account'])]
        else:
            d[data[item]['client']].append((item, data[item]['account']))
    an = set()
    for k, v in d.items():
        for i in range(0, len(v) - 1):
            if check_diff(v[i][1], v[i + 1][1]):
                an.add(v[i][0])
                an.add(v[i + 1][0])
    answer = sorted(an)
    for item in answer:
        client_rating[data[item]['client']] -= 5
        if f == 1:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_night_time(data: dict, client_rating: dict, limit, f=0):
    print('*****\ndetected for most operations at night')
    d = {}
    for item in data:
        if data[item]['client'] not in d.keys():
            d[data[item]['client']] = [(item, data[item]['date'].split('T')[1])]
        else:
            d[data[item]['client']].append((item, data[item]['date'].split('T')[1]))
    answer = []
    for k, v in d.items():
        if len(v) > 1:
            count = 0
            opers = []
            for item in v:
                if int(item[1].split(':')[0]) == 23 or int(item[1].split(':')[0]) < 6:
                    count += 1
                    opers.append(item[0])
            if float(count) / len(v) > limit:
                for item in opers:
                    client_rating[data[item]['client']] -= 3
                answer += opers
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_fast_operations(data: dict, client_rating: dict, time_interval: float, oper_number: int, f=0):
    print('*****\ndetected in carrying out multiple operations in a short period of time')
    answer = []
    oper=[]
    persons = []
    day = []
    hour = []
    for item in data:
        day.clear()
        hour.clear()
        counter = 1
        temp = str(data[item]['client'])
        oper.append(item)
        if temp not in persons:
            persons.append(temp)
            for i in data:
                if str(data[i]['client']) == temp:
                    day.append(data[i]['date'].split('T')[0].split('-')[2])
                    hour.append(data[i]['date'].split('T')[1])
                    oper.append(i)
            for i in range(len(day) - 1):
                if int(day[i]) < int(day[i + 1]) - 1:
                    i -= counter
                    counter = 1
                elif int(day[i]) * 1440 + int(hour[i].split(':')[0]) * 60 + int(hour[i].split(':')[1]) + int(
                        hour[i].split(':')[1]) / 60 + time_interval > int(day[i + 1]) * 1440 + int(
                    hour[i + 1].split(':')[0]) * 60 + int(hour[i + 1].split(':')[1]) + int(
                    hour[i + 1].split(':')[1]) / 60:
                    counter += 1
                else:
                    i -= counter - 1
                    counter = 1
                if counter == oper_number:
                    client_rating[temp]-=7
                    for j in range(i,i+counter):
                        answer.append(oper[j])
                    i+=counter
                    counter=1
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_reduction_of_the_amount(data: dict, client_rating: dict, f=0):
    print('*****\ndetected more than two consecutive withdrawal operations from the account')
    answer = []
    persons = {}
    for item in data:
        if data[item]['client'] not in persons:
            persons[data[item]['client']] = []
        persons[data[item]['client']].append(item)
        persons[data[item]['client']].append(data[item]['oper_type'])
        persons[data[item]['client']].append(data[item]['oper_result'])
        persons[data[item]['client']].append(data[item]['amount'])
    for per in persons:
        for i in range(len(persons[per]) - 4):
            if (persons[per][i+1] == 'Снятие' or persons[per][i+1] == 'Оплата') and (
                    persons[per][i + 5] == 'Снятие' or persons[per][i + 5] == 'Оплата'):
                if persons[per][i + 2] == 'Отказ' and persons[per][i + 6] == 'Отказ' and persons[per][i + 3] >= \
                        persons[per][i + 7]:
                    client_rating[per]-=7
                    answer.append(persons[per][i])
                    answer.append(persons[per][i+4])
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_age(data: dict):
    today = datetime.datetime.strptime(data['date'].split('T')[0], "%Y-%m-%d")
    birthdate = datetime.datetime.strptime(data['date_of_birth'].split('T')[0], "%Y-%m-%d")
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    if age < 33 or age > 65:
        return True
    else:
        return False


def check_personals(data, client_rating: dict, f=0):
    print('*****\ndetected in changing personals without changing passport')
    answer = []
    d = {}
    for item in data:
        if data[item]['client'] not in d.keys():
            d[data[item]['client']] = {(data[item]['last_name'], data[item]['first_name'], data[item]['patronymic'])}
        else:
            d[data[item]['client']].add((data[item]['last_name'], data[item]['first_name'], data[item]['patronymic']))
    for k, v in d.items():
        if len(v) > 1:
            client_rating[k] -= 7
            for item in data:
                if data[item]['client'] == k:
                    answer.append(item)
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_decline(data: dict, client_rating: dict, oper_number: int, f=0):
    print('*****\ndetected in carrying out many rejected operations')
    answer = []
    persons = {}
    for item in data:
        if data[item]['client'] not in persons:
            persons[data[item]['client']] = {'declined': 0, 'successfully': 0,'oper':[]}
        if data[item]['oper_result'] == 'Успешно':
            persons[data[item]['client']]['successfully'] += 1
        else:
            persons[data[item]['client']]['declined'] += 1
            persons[data[item]['client']]['oper'].append(item)
    for k in persons:
        if persons[k]['declined'] >= oper_number:
            client_rating[k]-=7
            for i in persons[k]['oper']:
                answer.append(i)
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer


def check_adress_distance(data, client_rating: dict, f=0):
    print('*****\ndetected in making transactions in too remote cities')
    cities, d = {}, {}
    create_txt = False
    wikipedia.set_lang("ru")
    if not exists('cities_cors.json'):
        create_txt = True
        print('downloading cities coordinatescoordinates...')
    else:
        with open('cities_cors.json', 'r') as inp:
            cities = dict(json.load(inp))
    for item in data:
        if data[item]['client'] not in d.keys():
            d[data[item]['client']] = [(item, data[item]['date'], data[item]['city'])]
        else:
            d[data[item]['client']].append((item, data[item]['date'], data[item]['city']))

        if create_txt:
            if data[item]['city'] not in cities.keys():
                try:
                    cor = wikipedia.page('город ' + data[item]['city']).coordinates
                    cities[data[item]['city']] = (float(cor[0]), float(cor[1]))
                except:
                    cities[data[item]['city']] = (0, 0)
    if create_txt:
        with open('cities_cors.json', 'w') as outfile:
            json.dump(cities, outfile)
    an = set()
    for k, v in d.items():
        for i in range(0, len(v) - 1):
            if cities[v[i][2]] != (0, 0) and cities[v[i + 1][2]] != (0, 0):
                t1 = datetime.datetime.strptime(v[i][1], "%Y-%m-%dT%H:%M:%S")
                t2 = datetime.datetime.strptime(v[i + 1][1], "%Y-%m-%dT%H:%M:%S")
                h = (geodesic(cities[v[i][2]], cities[v[i + 1][2]]).kilometers) / 750
                if t1 + datetime.timedelta(hours=h) > t2:
                    an.add(v[i][0])
                    an.add(v[i + 1][0])
                    client_rating[k] -= 25.5
    answer = sorted(an)
    if f == 1:
        for item in answer:
            print(data[item])
    print('total count: ', len(answer))
    return answer

def readall(l_n: str, f_n: str, pat: str):
    answer = {}
    a = (l_n, f_n, pat)
    cursor.execute(select_all, a)
    for item in cursor.fetchall():
        answer[item[0]] = {}
        for i in range(1, len(item)):
            answer[item[0]][pon[i]] = item[i]
    return answer


def readlastn(n: int, cl: str):
    answer = []
    a = (cl, n)
    cursor.execute(select_last_n, a)
    for item in cursor.fetchall():
        answer[item[0]] = {}
        for i in range(1, len(item)):
            answer[item[0]][pon[i]] = item[i]
    return answer