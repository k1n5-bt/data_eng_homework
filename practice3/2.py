import os
import json
from bs4 import BeautifulSoup
from common import safe_to_json

dir_path = 'data/2'
freq = {}
result = []

for filename in os.listdir(dir_path):
    abs_filename = os.path.join(dir_path, filename)
    with open(abs_filename, mode='r', encoding='utf-8') as html_fp:
        sp = BeautifulSoup(html_fp, 'html.parser')
        div_flex_wrap = sp.body.find("div", {"class": "list flex-wrap"})

        for div_pad in div_flex_wrap.find_all("div", {"class": "pad"}):
            item = dict()
            div_item = div_pad.find("div", {"class": "product-item"})

            links = div_item.find_all("a")
            data_id = links[0].get('data-id')
            href = links[1].get('href')

            l_attribute = item['link_attribute'] = dict()
            l_attribute['data_id'] = int(data_id)
            l_attribute['href'] = href

            img_url = div_pad.find("img")
            img_dict = item['img'] = dict()
            img_dict['loading'] = img_url.get('loading')
            img_dict['src'] = img_url.get('src')

            full_param = div_pad.find("span").text.split()
            device_param = item['device_param'] = {}
            device_param['inch'] = full_param[0][0:-1]
            device_param['company'] = " ".join(full_param[1:-1])
            device_param['flash'] = full_param[-1][0:-2]

            freq[device_param['company']] = freq[device_param['company']] + 1 if device_param['company'] in freq else 1
            item['price'] = int(''.join(div_pad.find('price').text.split()[:-1]))
            bonus_val = div_pad.find('strong').text.split()[2]
            item['bonus'] = int(bonus_val[0])
            ls = div_pad.find_all("li")

            prop = item['prop'] = dict()
            for i in ls:
                p = i.get('type')
                val = i.text.split()

                if p == 'processor':
                    prop['processor'] = {}
                    cpu = val[0].split('x')
                    prop['processor']['core'] = int(cpu[0])
                    prop['processor']['freq'] = float(cpu[1])
                elif p == 'resolution':
                    prop['resolution'] = i.text
                elif p == 'matrix':
                    prop['matrix'] = val[0]
                else:
                    prop[p] = int(val[0])
            result.append(item)

safe_to_json(result, 'out/2/out_final.json')

sorted_by_param = sorted(result, key=lambda x: x['prop']['camera'] if 'camera' in x['prop'] else 0, reverse=True)
safe_to_json(sorted_by_param, 'out/2/out_sorted.json')

filtered_list = [*filter(lambda x: x['price'] > 200000, result)]
safe_to_json(filtered_list, 'out/2/out_filter.json')

safe_to_json(freq, 'out/2/out_freq.json')

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
    'out/2/out_math_stat.json'
)
