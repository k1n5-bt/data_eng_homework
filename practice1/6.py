import requests
from bs4 import BeautifulSoup, Tag

URL = f"http://universities.hipolabs.com/search?country=Kazakhstan"

req = requests.get(URL)

data = req.json()

soup = BeautifulSoup()
for item in data:
    html = Tag(soup, name='html')
    table = Tag(soup, name='table')
    table['border'] = 1
    table['cellspacing'] = 1
    table['cellpadding'] = 1

    soup.append(html)
    html.append(table)
    for k, v in dict(item).items():
        tr = Tag(soup, name='tr')
        table.append(tr)
        tr.append(k)
        td = Tag(soup, name='td')
        tr.append(td)
        if v:
            if isinstance(v, list):
                for i in v:
                    td.append(i)
            else:
                td.append(v)
        else:
            td.append('-')

with open('data/universities.html', mode='w', encoding='utf8') as f:
    f.write(soup.prettify())

