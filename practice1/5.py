import csv
from bs4 import BeautifulSoup


with open('data/text_5_var_9', mode= 'r', encoding='utf-8') as f:
    soup = BeautifulSoup(f, 'html.parser')

with open('data/text_5_var_9_out', mode= 'w', encoding='utf-8') as f:
    table = soup.table
    rows = table.find_all('tr')
    writer = csv.writer(f, delimiter=',')
    for row in rows:
        col = row.find_all('td')
        if col:
            writer.writerow([item.text.strip() for item in col])