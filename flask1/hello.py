from flask import Flask
import json
import requests
import psycopg2

app = Flask(__name__)
connection = psycopg2.connect(
    host="db_auth",
    user="admin",
    password="root",
    database="postgres",
    port = 5432
)


def init(data: dict):
    for item in data:
        a = (item,)
        l = list(a)
        for i in data[item]:
            l.append(data[item][i])
        a = tuple(l)
        cursor.execute(add_command, a)


@app.route('/', methods=['POST','GET'])
def mail():
    request_data = request.json

    with connection.cursor() as cursor:
        init(request_data)
    return request_data


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7100, debug=True)

add_command="""
    INSERT INTO transactions VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
"""