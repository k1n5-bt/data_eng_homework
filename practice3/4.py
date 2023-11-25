import os
from bs4 import BeautifulSoup
from common import safe_to_json

dir_path = 'data/4'
freq = {}
result = []

for filename in os.listdir(dir_path):
    abs_filename = os.path.join(dir_path, filename)
    with open(abs_filename, mode='r', encoding='utf-8') as xml_fp:
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

safe_to_json(result, 'out/4/out_final.json')

sorted_list = sorted(result, key=lambda x: x['price'])
safe_to_json(sorted_list, 'out/4/out_sorted.json')

filtered_list = [*sorted(filter(lambda x: x['rating'] < 2.0, result), key=lambda x: x['rating'], reverse=True)]
safe_to_json(filtered_list, 'out/4/out_filter.json')

safe_to_json(freq, 'out/4/out_freq.json')

sum_c = 0
for i in result:
    sum_c += i['price']
safe_to_json(
    [
        f"Min price: {min(result, key=lambda x: x['price'])['price']}",
        f"Max price: {max(result, key=lambda x: x['price'])['price']}",
        f"Sum price: {sum_c}",
        f"Avg price: {sum_c / len(result)}",
    ],
    'out/4/out_math_stat.json'
)
