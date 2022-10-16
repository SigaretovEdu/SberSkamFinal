from flask import Flask, request
import json
import requests
import psycopg2
import time


initcommand="""CREATE TABLE IF NOT EXISTS transactions (
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

add_command="""
    INSERT INTO transactions (date,card,account,account_valid_to,client,last_name,first_name,patronymic,date_of_birth,passport,passport_valid_to,phone,oper_type,amount,oper_result,terminal,terminal_type,city,address) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""

app = Flask(__name__)



def init(data: dict):
    l=[]
    for item in data:
        l.append(data[item])
    a = tuple(l)
    return a


@app.route('/', methods=['POST','GET'])
def mail():
    request_data = request.json
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
        port = 5432
    )
    with connection.cursor() as cursor:
        cursor.execute(initcommand)
    connection.commit()
    app.run(host="0.0.0.0", port=7100, debug=True)