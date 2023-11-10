from json import load
from pickle import dump as p_dump, load as p_load

input_json_filename = 'data/price_info_9.json'
input_pkl_filename = 'data/products_9.pkl'
methods = {
    'add': lambda x, y: x + y,
    'sub': lambda x, y: x - y,
    'percent+': lambda x, y: x * (1 + y),
    'percent-': lambda x, y: x * (1 - y),
}

with open(input_json_filename, mode='r') as f:
    data = load(f)

with open(input_pkl_filename, mode='rb') as f:
    items = p_load(f)

name_to_price_info = {i['name']: i for i in data}

for item in items:
    name = item['name']
    method = name_to_price_info[name]['method']
    if method in methods:
        item['price'] = methods[method](item['price'], name_to_price_info[name]['param'])

with open('data/products_9_out.pkl', 'wb') as f:
    p_dump(items, f)
