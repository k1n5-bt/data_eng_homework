import json
from datetime import datetime
from bs4 import BeautifulSoup
from requests import get

url = 'https://vsalde.ru/page'
result = []
months = [
  'января',
  'февраля',
  'марта',
  'апреля',
  'мая',
  'июня',
  'июля',
  'августа',
  'сентября',
  'октября',
  'ноября',
  'декабря'
]
freq = {}

for page in range(1, 11):
    site_page = get(f'{url}/{page}')
    sp = BeautifulSoup(site_page.text, 'html.parser')
    div_dle = sp.body.find('div', {'id': 'dle-content'})

    for div_pad in div_dle.find_all('div', {'class': 'news-main'}):
        item = dict()
        div_item = div_pad.find('div', {'class': 'news-story'})

        href = div_pad.find('div', {'class': 'news-more'}).find('a').get('href')
        item['href'] = href

        title = div_item.find_all('div')[0].find_all('img')[0].get('title')
        item['title'] = title

        img = div_item.find_all('div')[0].find_all('img')[0].get('src')
        item['img'] = img

        date_text = div_pad.find('div', {'class': 'news-info'}).find('div', {'class': 'news-other'}).text.split(',')[1].split('|')[0].strip()
        date_text = f'{date_text.split()[0]}.{months.index(date_text.split()[1]) + 1}.{date_text.split()[2]}'
        freq[date_text.split('.')[1]] = freq[date_text.split('.')[1]] + 1 if date_text.split('.')[1] in freq else 1
        item['date'] = date_text

        object_site = get(href)
        sp1 = BeautifulSoup(object_site.text, 'html.parser')
        div_dle1 = sp1.body.find('div', {'id': 'dle-content'})
        text = div_dle1.find('div', {'class': 'news-story'}).find('div').text
        item['text'] = text
        result.append(item)

with open('out/5/out_final.json', mode='w') as f:
    json.dump(result, f, ensure_ascii=False)

with open('out/5/out_sorted.json', mode='w') as f:

    json.dump(sorted(result, key=lambda x: datetime.strptime(x['date'], "%d.%m.%Y"), reverse=True), f, ensure_ascii=False)

with open('out/5/out_filter.json', mode='w') as f:
    filtered_list = sorted(result, key=lambda x: datetime.strptime(x['date'], "%d.%m.%Y") < datetime.strptime('10.11.2023', "%d.%m.%Y"), reverse=True)
    json.dump([*filtered_list], f, ensure_ascii=False)

with open('out/5/out_freq.json', mode='w') as f:
    json.dump(freq, f, ensure_ascii=False)

print('Min date: ', min(result, key=lambda x: x['date'])['date'])
print('Max date: ', max(result, key=lambda x: x['date'])['date'])
