import datetime
import json
import requests

url = 'http://192.168.240.144:7100'
headers = {"charset": "utf-8", 'Content-type': 'application/json', 'Accept': 'text/plain'}

with open("transactions_final.json", 'r', encoding='utf-8') as f:
    data = dict(json.load(f)['transactions'])

count = 0
time_now = datetime.datetime.now()
sum_time = datetime.datetime.now()
for item in data:
    count += 1
    d = {'id': int(item)}
    for k, v in data[item].items():
        d[k] = v
    d['amount'] = float(d['amount'])
    d['passport'] = str(d['passport'])

    response = requests.post(url, data=json.dumps(d), headers=headers)
    if count % 100 == 0:
        print(count, datetime.datetime.now()-time_now)
        time_now = datetime.datetime.now()

    # print(count, item, response, d)
    # if count == 10:
    #     break
print(f'end total:{count}')
print(datetime.datetime.now()-sum_time)