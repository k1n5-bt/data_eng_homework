from json import load, dump
import msgpack
from os.path import getsize

out_json_filename = 'data/products_9_out.json'
out_msgpack_filename = 'data/products_9_out.json.msgpack'
data = load(open('data/products_9.json', 'r'))
items = {}
result = []

for item in data:
    items[item['name']] = [item['price']] if item['name'] not in items else items[item['name']] + [item['price']]

for name, prices in items.items():
    result.append({
        "name": name,
        "max": max(prices),
        "min": min(prices),
        "avr": sum(prices) / len(prices),
    })


with open(out_json_filename, mode='w') as f:
    dump(result, f)

with open(out_msgpack_filename, mode='wb') as f:
    msgpack.dump(result, f)

print(f'Json file size: {getsize(out_json_filename)}\n'
      f'Msgpack file size: {getsize(out_msgpack_filename)}')
