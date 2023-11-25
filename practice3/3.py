import os
from bs4 import BeautifulSoup
from common import safe_to_json

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

safe_to_json(result, 'out/3/out_final.json')

sorted_list = sorted(result, key=lambda x: x['age'], reverse=True)
safe_to_json(sorted_list, 'out/3/out_sorted.json')

filtered_list = [*filter(lambda x: x['age'] > 3 * (10 ** 9), result)]
safe_to_json(filtered_list, 'out/3/out_filter.json')

safe_to_json(freq, 'out/3/out_freq.json')

sum_c = 0
for i in result:
    sum_c += i['distance']
safe_to_json(
    [
        f"Min distance: {min(result, key=lambda x: x['distance'])['distance']}",
        f"Max distance: {max(result, key=lambda x: x['distance'])['distance']}",
        f"Sum distance: {sum_c}",
        f"Avg distance: {sum_c / len(result)}",
    ],
    'out/3/out_math_stat.json',
)
