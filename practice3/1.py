import os
import json
from bs4 import BeautifulSoup

dir_path = 'data/1'
freq = {}
result = []

for filename in os.listdir(dir_path):
    abs_filename = os.path.join(dir_path, filename)
    item = dict()
    item['filename'] = abs_filename
    with open(abs_filename, mode='r') as html_fp:
        sp = BeautifulSoup(html_fp, 'html.parser')
        div_iter = iter(sp.find("div", {"class": "chess-wrapper"}).find_all("div"))

        elem_type = next(div_iter)
        item['type'] = elem_type.find("span").text.split(':')[1].strip()

        elem_tournament = next(div_iter)
        tournament = elem_tournament.find("h1", {"class": "title", "id": "1"})
        item['tournament'] = {}
        tournament = tournament.text.split(':')[1].strip()

        city = ' '.join(tournament.split()[:-1])
        item['tournament']['city'] = city
        item['tournament']['year'] = int(tournament.split()[-1])
        freq[city] = freq[city] + 1 if city in freq else 1

        location = item['location'] = {}
        info = elem_tournament.find("p", {'class': 'address-p'})

        info = info.text.replace("Город:", '').replace("Начало:", ' ').split()

        location['city'] = info[0]
        location['date'] = info[1]

        elem_game_info = next(div_iter)
        span_iter = iter(elem_game_info.find_all("span"))

        game_info = item['game_info'] = {}

        tournament_count = next(span_iter)

        game_info['tournament_count'] = int(tournament_count.text.split(':')[1].strip())

        tournament_time = next(span_iter)

        game_info['tournament_time'] = int(tournament_time.text.split(':')[1].strip().split()[0])

        min_rating = next(span_iter)
        game_info['min_rating'] = int(min_rating.text.split(':')[1].strip())

        img_src = next(div_iter)

        img_url_f = img_src.find('img')

        img_url_a = img_url_f.get('src') if img_url_f else ''

        item['photo'] = img_url_a

        rating = next(div_iter)
        rating_iter = iter(rating.find_all('span'))

        rating_info = next(rating_iter)
        item['rating_info'] = {}
        item['rating_info']['rating'] = float(rating_info.text.split(':')[1].strip())

        views = next(rating_iter)

        item['rating_info']['views'] = int(views.text.split(':')[1].strip())

        result.append(item)

with open("out/1/out_final.json", mode='w') as f:
    json.dump(result, f, ensure_ascii=False)

with open("out/1/out_sorted.json", mode='w') as f:
    sorted_ls = sorted(result, key=lambda x: x['rating_info']['rating'], reverse=True)
    json.dump(sorted_ls, f, ensure_ascii=False)

with open("out/1/out_filter.json", mode='w') as f:
    json.dump([*filter(lambda x: x['tournament']['year'] > 2010, result)], f, ensure_ascii=False)

with open("out/1/out_freq.json", mode='w') as f:
    json.dump(freq, f, ensure_ascii=False)

print('Min tournament_count: ', min(result, key=lambda x: x['game_info']['tournament_count'])['game_info']['tournament_count'])
print('Max tournament_count: ', max(result, key=lambda x: x['game_info']['tournament_count'])['game_info']['tournament_count'])
sum_c = 0
for i in result:
    sum_c += i['game_info']['tournament_count']
print('Sum tournament_count: ', sum_c)
print('Avg tournament_count: ', sum_c / len(result))
