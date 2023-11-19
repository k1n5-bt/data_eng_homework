import math
import os
import json
from bs4 import BeautifulSoup

dir_path = 'data/3'
freq = {}
result = []

for filename in os.listdir(dir_path):
    abs_filename = os.path.join(dir_path, filename)
    item = dict()
    with open(abs_filename, mode='r', encoding='utf-8') as xml_fp:
        sp = BeautifulSoup(xml_fp, features='xml')

        obj = sp.find("star")
        name = obj.find("name").text
        item['star_name'] = name.strip()

        item['constellation'] = obj.find("constellation").text.strip()

        freq[item['constellation']] = freq[item['constellation']] + 1 if item['constellation'] in freq else 1

        spec_class = obj.find("spectral-class").text
        item['spectral_class'] = spec_class.strip()

        radius = obj.find('radius').text
        item['radius'] = int(radius.strip())

        rotation = obj.find('rotation').text.split()[0]
        item['rotation'] = float(rotation)

        age = obj.find("age").text.split()
        item['age'] = float(age[0]) * 10 ** 6 if age[1] == 'million' else float(age[0]) * 10**9

        distance = obj.find("distance").text.split()
        item['distance'] = float(distance[0]) * 10 ** 6 if distance[1] == 'million' else float(distance[0]) * 10 ** 9

        magnitude = obj.find('absolute-magnitude').text.split()
        item['magnitude'] = float(magnitude[0]) * 10 ** 6 if magnitude[1] == 'million' else float(magnitude[0]) * 10 ** 9

    result.append(item)


with open('out/3/out_final.json', mode='w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False)

with open('out/3/out_sorted.json', mode='w', encoding='utf-8') as f:
    json.dump(sorted(result, key=lambda x: x['age'], reverse=True), f, ensure_ascii=False)

with open('out/3/out_filter.json', mode='w', encoding='utf-8') as f:
    filtered_list = filter(lambda x: x['age'] > 3 * (10 ** 9), result)
    json.dump([*filtered_list], f, ensure_ascii=False)

with open('out/3/out_freq.json', mode='w', encoding='utf-8') as f:
    json.dump(freq, f, ensure_ascii=False)


print('Min distance: ', min(result, key=lambda x: x['distance'])['distance'])
print('Max distance: ', max(result, key=lambda x: x['distance'])['distance'])
sum_c = 0
for i in result:
    sum_c += i['distance']
print('Sum distance: ', sum_c)
print('Avg distance: ', sum_c / len(result))
