import os
import json
from bs4 import BeautifulSoup

dir_path = 'data/4'
freq = {}
result = []

for filename in os.listdir(dir_path):
    abs_filename = os.path.join(dir_path, filename)
    with open(abs_filename, mode='r') as xml_fp:
        sp = BeautifulSoup(xml_fp, features="xml")
        items = sp.find("clothing-items")
        for val in items.find_all("clothing"):
            tags = list(val.children)
            item = dict()
            for tag in tags:
                if tag.name:
                    tag_name = tag.name.strip()
                    tag_value = tag.text.strip()
                    if tag_name in ('price', 'reviews', 'id', 'rating'):
                        tag_value = int(tag_value) if tag_name != 'rating' else float(tag_value)
                    item[tag_name] = tag_value
            freq[item['size']] = freq[item['size']] + 1 if item['size'] in freq else 1
            result.append(item)


with open('out/4/out_final.json', mode='w') as f:
    json.dump(result, f, ensure_ascii=False)

with open('out/4/out_sorted.json', mode='w') as f:
    json.dump(sorted(result, key=lambda x: x['price'], reverse=True), f, ensure_ascii=False)

with open('out/4/out_filter.json', mode='w') as f:
    filtered_list = sorted(filter(lambda x: x['rating'] < 2.0, result), key=lambda x: x['rating'], reverse=True)
    json.dump([*filtered_list], f, ensure_ascii=False)

with open('out/4/out_freq.json', mode='w') as f:
    json.dump(freq, f, ensure_ascii=False)

print('Min price: ', min(result, key=lambda x: x['price'])['price'])
print('Max price: ', max(result, key=lambda x: x['price'])['price'])
sum_c = 0
for i in result:
    sum_c += i['price']
print('Sum price: ', sum_c)
print('Avg price: ', sum_c / len(result))
