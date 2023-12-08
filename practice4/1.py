from json import load
from common import create_connection, insert_table, get_data, get_stat_data, get_freq, drop_table

dbname = "db/1/1.db"
v = 9

with open('data/1/task_1_var_09_item.json', mode='r') as f:
    data = load(f)

table = 'tournaments'
global conn
global cursor

try:
    conn, cursor = create_connection(dbname)

    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table}
                       (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, city TEXT, begin TEXT, system TEXT,
                       tours_count INTEGER, min_rating INTEGER, time_on_game INTEGER)
                    """)

    insert_table(cursor, data, table)
    conn.commit()

    get_data(cursor, table, 'out/1/1.json', sort_by='min_rating', limit=v+10)

    get_stat_data(cursor, table, 'out/1/2.json', 'time_on_game')

    get_freq(cursor, table, 'out/1/3.json', 'city')

    get_data(cursor, table, 'out/1/4.json', sort_by='min_rating', limit=v+10, filtered_field='tours_count', filtered_value=7)

    print('Данные успешно записаны в директорию ./out/1')
except Exception as exc:
    print(f'Exception: {exc}')
    print('Ошибка при работе с данными')
finally:
    drop_table(cursor, table)
    conn.close()
