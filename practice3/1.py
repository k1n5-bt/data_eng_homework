import os
import json
from bs4 import BeautifulSoup
from common import safe_to_json

dir_path = 'data/1'
freq = {}
result = []

for filename in os.listdir(dir_path):
    abs_filename = os.path.join(dir_path, filename)
    item = dict()
    item['filename'] = abs_filename
    with open(abs_filename, mode='r', encoding='utf-8') as html_fp:
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

safe_to_json(result, 'out/1/out_final.json')

sorted_list = sorted(result, key=lambda x: x['rating_info']['rating'], reverse=True)
safe_to_json(
    sorted_list,
    'out/1/out_sorted.json',
)

filtered_list = [*filter(lambda x: x['tournament']['year'] > 2010, result)]
safe_to_json(
    filtered_list,
    'out/1/out_filter.json',
)

safe_to_json(
    freq,
    'out/1/out_freq.json',
)

sum_c = 0
for i in result:
    sum_c += i['game_info']['tournament_count']
safe_to_json(
    [
        f"Min tournament count: {min(result, key=lambda x: x['game_info']['tournament_count'])['game_info']['tournament_count']}",
        f"Max tournament count: {max(result, key=lambda x: x['game_info']['tournament_count'])['game_info']['tournament_count']}",
        f"Sum tournament count: {sum_c}",
        f"Avg tournament count: {sum_c / len(result)}"
    ],
    'out/1/out_math_stat.json'
)
