import psycopg2, json
from config import *

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name,
    port=port
)
with open("transactions.json", 'r', encoding='utf-8') as f:
    data = dict(json.load(f)['transactions'])


def init(data: dict):
    for item in data:
        a = (item,)
        l = list(a)
        for i in data[item]:
            l.append(data[item][i])
        a = tuple(l)
        cursor.execute(add_command, a)


with connection.cursor() as cursor:
    cursor.execute(init_command)
    init(data)
    # cursor.execute("""DROP TABLE transactions;""")

connection.commit()
connection.close()
