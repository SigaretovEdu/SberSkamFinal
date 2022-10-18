import requests
from flask import Flask, request
import json
import psycopg2
import time
import wikipedia
from os.path import exists
from geopy.distance import geodesic
import datetime
import logging


app = Flask(__name__)

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


def check_failed_passport_validation(data: dict):
    t1 = datetime.datetime.strptime(data['date'].split('T')[0], "%Y-%m-%d")
    t2 = datetime.datetime.strptime(data['passport_valid_to'].split('T')[0], "%Y-%m-%d")
    if t1 > t2:
        return True
    else:
        return False


def check_failed_account_validation(data: dict):
    t1 = datetime.datetime.strptime(data['date'].split('T')[0], "%Y-%m-%d")
    t2 = datetime.datetime.strptime(data['account_valid_to'].split('T')[0], "%Y-%m-%d")
    if t1 > t2:
        return True
    else:
        return False


def check_brute(l: list):
    if check_diff(l[0]['account'], l[1]['account']) and check_diff(l[1]['account'], l[2]['account']):
        return True
    else:
        return False


def check_night_time(data: dict):
    t = datetime.datetime.strptime(data['date'].split('T')[1], "%H:%M:%S")
    t = t.hour
    if t == 23 or t < 6:
        return True
    else:
        return False


def check_fast_operations(l: list, delta: int):
    pause = datetime.timedelta(minutes=0)
    for i in range(0, len(l) - 1):
        t1 = datetime.datetime.strptime(l[i]['date'], "%Y-%m-%dT%H:%M:%S")
        t2 = datetime.datetime.strptime(l[i + 1]['date'], "%Y-%m-%dT%H:%M:%S")
        pause += t2 - t1
    ex = (len(l) - 1) * delta
    ex = datetime.timedelta(minutes=ex)
    if pause < ex:
        return True
    else:
        return False


def check_reduction_of_the_amount(l: list):
    if l[0]['oper_result'] == 'Отказ' and l[1]['oper_result'] == 'Отказ':
        if l[0]['amount'] * 2 < l[1]['amount']:
            return True
    else:
        return False


def check_age(data: dict):
    today = datetime.datetime.strptime(data['date'].split('T')[0], "%Y-%m-%d")
    birthdate = datetime.datetime.strptime(data['date_of_birth'].split('T')[0], "%Y-%m-%d")
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    if age < 33 or age > 65:
        return True
    else:
        return False


def check_decline(l: list):
    if l[0]['oper_result'] == 'Отказ' and l[1]['oper_result'] == 'Отказ' and l[2]['oper_result'] == 'Отказ':
        return True
    else:
        return False


def check_adress_distance(l:list):
    with connection.cursor() as cursor:
        cursor.execute(matches, (l[0]['city'],))
        town1=bool(cursor.fetchone()[0])
    with connection.cursor() as cursor:
        cursor.execute(matches, (l[1]['city'],))
        town2=bool(cursor.fetchone()[0])
        
        wikipedia.set_lang("ru")
        if not town1:
            try:
                cor = wikipedia.page('город ' + l[0]['city']).coordinates
            except:
                cor = (0,0)
            cursor.execute("""INSERT INTO city_coords VALUES(%s,%s,%s) ON CONFLICT (city_name) DO NOTHING""",(l[0]['city'],cor[0],cor[1]))
        if not town2:
            try:
                cor = wikipedia.page('город ' + l[1]['city']).coordinates
            except:
                cor = (0,0)
            cursor.execute("""INSERT INTO city_coords VALUES(%s,%s,%s) ON CONFLICT (city_name) DO NOTHING""",(l[1]['city'],cor[0],cor[1]))
    with connection.cursor() as cursor:
        cursor.execute("""SELECT x_deg, y_deg FROM city_coords WHERE city_name=%s""",(l[0]['city'],))
        temp1=cursor.fetchone()
    with connection.cursor() as cursor:
        cursor.execute("""SELECT x_deg, y_deg FROM city_coords WHERE city_name=%s""",(l[1]['city'],))
        temp2=cursor.fetchone()
    if temp1!=(0,0) and temp2!=(0,0):
        t1 = datetime.datetime.strptime(l[0]['date'], "%Y-%m-%dT%H:%M:%S")           
        t2 = datetime.datetime.strptime(l[1]['date'], "%Y-%m-%dT%H:%M:%S")
        h = (geodesic(temp1, temp2).kilometers) / 750
        if t1 + datetime.timedelta(hours=h) > t2:
            return True
    return False

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
    with connection.cursor() as cursor:
        cursor.execute(select_last_n, a)
        roar=cursor.fetchall()
        for pars in roar:
            d = {"id": int(pars[0]),
                "date": pars[1],
                "card": pars[2],
                "account": pars[3],
                "account_valid_to": pars[4],
                "client": pars[5],
                "last_name": pars[6],
                "first_name": pars[7],
                "patronymic": pars[8],
                "date_of_birth": pars[9],
                "passport": pars[10],
                "passport_valid_to": pars[11],
                "phone": pars[12],
                "oper_type": pars[13],
                "amount": float(pars[14]),
                "oper_result": pars[15],
                "terminal": pars[16],
                "terminal_type": pars[17],
                "city": pars[18],
                "address": pars[19]
                }
            answer.append(d)
    return answer


frod_types=['to_old_or_young',
'same_card_num',
'fast_operations',
'many_declines',
'decreasing_operation_sum',
'invalid_passport',
'account_validation',
'dist']

city_c="""CREATE TABLE IF NOT EXISTS city_coords (
    city_name CHARACTER VARYING (25) PRIMARY KEY,
    x_deg REAL,
    y_deg REAL
);"""

matches="""SELECT EXISTS (SELECT city_name FROM city_coords WHERE city_name = %s);"""

rating = """CREATE TABLE IF NOT EXISTS client_rating (
    client_id CHARACTER VARYING (10) PRIMARY KEY,
    rating REAL,
    day_operations INT,
    night_operations INT,
    age INT,
    mult INT
);"""

validat = """CREATE TABLE IF NOT EXISTS validation (
    passport CHARACTER VARYING (25) PRIMARY KEY,
    passport_valid_to CHARACTER VARYING (25)
);"""

check_valid="""SELECT passport_valid_to FROM validation WHERE passport = %s AND passport_valid_to != %s;"""

initcommand = """CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGINT PRIMARY KEY,
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
    SELECT * FROM transactions WHERE client=%s;
"""

select_last_n = """ 
    SELECT * FROM transactions WHERE client=%s ORDER BY transaction_id DESC LIMIT %s;  
"""

pon = (
    'transaction_id', 'date', 'card', 'account', 'account_valid_to', 'client', 'last_name', 'first_name',
    'patronymic',
    'date_of_birth', 'passport', 'passport_valid_to', 'phone', 'oper_type', 'amount', 'oper_result', 'terminal',
    'terminal_type', 'city', 'address')

add_command1 = """
    INSERT INTO transactions VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (transaction_id) DO NOTHING;
"""
add_command2 = """
    INSERT INTO client_rating VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT (client_id) DO NOTHING;
"""
add_command3 = """
    INSERT INTO validation VALUES(%s,%s) ON CONFLICT (passport) DO NOTHING;
"""

def init(data: dict):
    l = []
    for item in data:
        l.append(data[item])
    a = tuple(l)
    return a


@app.route('/', methods=['POST', 'GET'])
def mail():
    request_data = request.json
    valid_data = True
    for v in request_data.values():
        if v == '':
            print('data error', request_data['client'])
            valid_data = False

    if valid_data:
        with connection.cursor() as cursor:
            cursor.execute(add_command1, init(request_data))
            cursor.execute(add_command2,(request_data['client'],100,0,0,0,0))
            cursor.execute(add_command3,(request_data['passport'],request_data['passport_valid_to']))

        
        last=readlastn(3,request_data['client'])
        if len(last)==3:
            last[0],last[2]=last[2],last[0] 
        bill = 0

        # age
        if check_age(last[0]):
            with connection.cursor() as cursor:
                cursor.execute("""UPDATE client_rating SET age=1 WHERE client_id=%s;""",(request_data['client'], ))
                cursor.execute("""INSERT INTO to_old_or_young (transaction_id) VALUES (%s) ON CONFLICT (transaction_id) DO NOTHING;""", (request_data['id'],))

        
        # multiple validation
        with connection.cursor() as cursor:
            cursor.execute(check_valid,(request_data['passport'],request_data['passport_valid_to']))
            s_valids = cursor.fetchall()
        mult_val = False
        for item in s_valids:
            if str(item) != request_data['passport_valid_to']:
                mult_val = True
                break
        if mult_val:
            bill += 0
            with connection.cursor() as cursor:
                cursor.execute("""UPDATE client_rating SET mult=1 WHERE client_id=%s;""",(request_data['client'], ))

        # night
        if_night = check_night_time(last[0])
        with connection.cursor() as cursor:
            if if_night:
                cursor.execute("""UPDATE client_rating SET night_operations = night_operations + 1 WHERE client_id=%s""",(request_data['client'],))
            else:
                cursor.execute("""UPDATE client_rating SET day_operations = day_operations + 1 WHERE client_id=%s""",(request_data['client'],))

        # brute
        if len(last) == 3:
            if check_brute(last):
                bill += 0
                with connection.cursor() as cursor:
                    cursor.execute("""INSERT INTO same_card_num VALUES (%s) ON CONFLICT (transaction_id) DO NOTHING""",(request_data['id'],))

        # ddos
        if len(last) == 3:
            if check_fast_operations(last,3):
                bill += 0
                with connection.cursor() as cursor:
                    cursor.execute("""INSERT INTO fast_operations VALUES (%s) ON CONFLICT (transaction_id) DO NOTHING""",(request_data['id'],))

        # rejections
        if len(last) == 3:
            if check_decline(last):
                bill += 0
                with connection.cursor() as cursor:
                    cursor.execute("""INSERT INTO many_declines VALUES (%s) ON CONFLICT (transaction_id) DO NOTHING""",(request_data['id'],))

        # amount less
        if len(last) == 3:
            if check_reduction_of_the_amount(last):
                bill += 0
                with connection.cursor() as cursor:
                    cursor.execute("""INSERT INTO decreasing_operation_sum VALUES (%s) ON CONFLICT (transaction_id) DO NOTHING""",(request_data['id'],))

        # invalid passport
        if check_failed_passport_validation(last[0]):
            bill += 0
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO invalid_passport VALUES (%s) ON CONFLICT (transaction_id) DO NOTHING""",(request_data['id'],))

        # invalid account
        if check_failed_account_validation(last[0]):
            bill += 0
            with connection.cursor() as cursor:
                cursor.execute("""INSERT INTO account_validation VALUES (%s) ON CONFLICT (transaction_id) DO NOTHING""",(request_data['id'],))

        # city
        if len(last)>1:
            if check_adress_distance(last):
                bill += 0
                with connection.cursor() as cursor:
                    cursor.execute("""INSERT INTO dist VALUES (%s) ON CONFLICT (transaction_id) DO NOTHING""",(request_data['id'],))



        with connection.cursor() as cursor:
            cursor.execute("""UPDATE client_rating SET rating=rating-%s WHERE client_id=%s""",(bill,request_data['client']))
        
        connection.commit()

        return 'good request', 200
    else:
        return 'bad request', 400


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
        cursor.execute(city_c)
        for i in frod_types:
            cursor.execute("""CREATE TABLE IF NOT EXISTS {tab} (
                transaction_id BIGINT PRIMARY KEY
                );""".format(tab=i))
    connection.commit()
    app.run(host="0.0.0.0", port=7100, debug=True,threaded=True)
