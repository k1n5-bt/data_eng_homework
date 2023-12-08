from msgpack import unpackb
import csv
from common import create_connection, drop_table, insert_table, get_data, get_stat_data, get_freq

v = 9
filename1 = "data/3/task_3_var_09_part_1.csv"
filename2 = "data/3/task_3_var_09_part_2.msgpack"

dbname = "db/3/3.db"
table = 'music'
data = []

with open(filename2, 'rb') as f:
    for i in unpackb(f.read()):
        data.append(
            {
                'artist': i['artist'], 'song': i['song'], 'duration_ms': int(i['duration_ms']),
                'year': int(i['year']), 'tempo': float(i['tempo']), 'genre': i['genre'],
            }
        )

with open(filename1, 'r') as f:
    item = csv.DictReader(f, delimiter=';')
    for i in item:
        data.append(
            {
                'artist': i['artist'], 'song': i['song'], 'duration_ms': int(i['duration_ms']),
                'year': int(i['year']), 'tempo': float(i['tempo']), 'genre': i['genre'],
            }
        )


global conn
global cursor
if __name__ == "__main__":
    try:
        conn, cursor = create_connection(dbname)
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table}
                        (id INTEGER PRIMARY KEY, artist TEXT, song TEXT, duration_ms INTEGER, year INTEGER, tempo NUMBER,
                        genre TEXT)
                        """)

        insert_table(cursor, data, table)
        conn.commit()

        get_data(cursor, table, 'out/3/1.json', sort_by='year', limit=v + 10)

        get_stat_data(cursor, table, 'out/3/2.json', 'duration_ms')

        get_freq(cursor, table, 'out/3/3.json', 'artist')

        get_data(cursor, table, 'out/3/4.json', sort_by='duration_ms', limit=v + 15, filtered_field='year', filtered_value=2000)

        print('Данные успешно записаны в директорию ./out/3')
    except Exception as exc:
        print(f'Exception: {exc}')
        print('Ошибка при работе с данными')
    finally:
        drop_table(cursor, table)
        conn.close()
