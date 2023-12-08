from common import create_connection, insert_table, drop_table, write_file
from json import load

dbname = 'db/2/2.db'

table_1 = 'tournaments'
table_2 = 'tournaments_tickets'

with open('data/1/task_1_var_09_item.json', mode='r') as f:
    data_1 = load(f)

with open('data/2/task_2_var_09_subitem.json', mode='r') as f:
    data_2 = load(f)


def get_tickets_by_city(cursor, city):
    query = f"""SELECT * FROM {table_2} WHERE tour_id in (SELECT id FROM {table_1} WHERE city = ?)"""
    data = cursor.execute(query, [city]).fetchall()
    write_file([dict(i) for i in data], 'out/2/1.json')


def get_max_prices_by_system(cursor, system=None):
    query = f"""SELECT name, MAX(prise) as max_price FROM {table_2} WHERE tour_id in (SELECT id FROM {table_1} WHERE system = ?) GROUP BY name"""
    data = cursor.execute(query, [system]).fetchall()
    write_file([dict(i) for i in data], 'out/2/2.json')


def get_count_tickets_by_rating(cursor, rating):
    query = f"""SELECT name, COUNT(name) as count FROM {table_2} WHERE tour_id in (SELECT id FROM {table_1} WHERE min_rating >= ?) GROUP BY name"""
    data = cursor.execute(query, [rating]).fetchall()
    write_file([dict(i) for i in data], 'out/2/3.json')


global conn
global cursor
if __name__ == "__main__":
    try:
        conn, cursor = create_connection(dbname)
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_1}
                            (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, city TEXT, begin TEXT, system TEXT,
                            tours_count INTEGER, min_rating INTEGER, time_on_game INTEGER)
                        """)

        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_2} 
                           (tour_id REFERENCES {table_1} (id), name TEXT, place INTEGER, prise INTEGER)
                        """)

        insert_table(cursor, data_1, table_1)
        conn.commit()

        response = cursor.executemany(
            f"""INSERT INTO {table_2} (tour_id, name, place, prise)
                VALUES((SELECT id from {table_1} WHERE name = :name), :name, :place, :prise) """,
            data_2,
        )
        conn.commit()

        get_tickets_by_city(cursor, 'Трухильо')
        get_max_prices_by_system(cursor, 'Olympic')
        get_count_tickets_by_rating(cursor, 2400)

        print('Данные успешно записаны в директорию ./out/2')
    except Exception as exc:
        print(f'Exception: {exc}')
        print('Ошибка при работе с данными')
    finally:
        drop_table(cursor, table_1)
        drop_table(cursor, table_2)
        conn.close()
