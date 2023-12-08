import pickle
from common import create_connection, drop_table, insert_table, write_file

filename1 = "data/4/task_4_var_09_product_data.pkl"
filename2 = "data/4/task_4_var_09_update_data.text"

dbname = "db/4/4.db"
table = 'products'

with open(filename1, 'rb') as f:
    products_data = pickle.load(f)


with open(filename2, mode='r') as f:
    update_data = []
    text = f.read()
    for i in text.split('====='):
        item = {}
        for j in i.split('\n'):
            if j:
                k, v = j.split('::')
                item[k] = v
        if item:
            update_data.append(item)


def update_database(cursor, table, data):
    for i in data:
        method = i['method']
        name = i['name']
        param = i['param']
        response = None

        if method == 'available':
            response = cursor.execute(
                f"UPDATE {table} SET isAvailable = ?, counter = counter + 1 WHERE name == ?",
                [bool(param), name],
            )
        elif method == 'quantity_sub':
            response = cursor.execute(
                f"UPDATE {table} SET quantity = MAX(quantity - ?, 0), counter = counter + 1 "
                f"WHERE name = ? AND ((quantity - ?) > 0)",
                [param, name, param],
            )
        elif method == 'quantity_add':
            response = cursor.execute(
                f"UPDATE {table} SET quantity = quantity + ? WHERE name = ?",
                [param, name],
            )
        elif method == 'price_percent':
            response = cursor.execute(
                f"UPDATE {table} SET price = ROUND(price * (1 + ?), 2), counter = counter + 1 WHERE name = ?",
                [param, name],
            )
        elif method == 'remove':
            response = cursor.execute(f"DELETE FROM {table} WHERE name = ?", [name])
        elif method == 'price_abs':
            response = cursor.execute(
                f"UPDATE {table} SET price = MAX(price + ?, 0) WHERE name = ? AND ((price + ?) > 0)",
                [param, name, param],
            )

        if response and response.rowcount > 0:
            cursor.execute(f"UPDATE {table} SET counter = counter + 1 WHERE name = ?", [name])


def get_top_updated(cursor, table):
    response = cursor.execute(f"SELECT name, counter FROM {table} ORDER BY counter DESC LIMIT 10")

    write_file([dict(r) for r in response.fetchall()], 'out/4/1.json')


def get_prices_by_category(cursor, table):
    response = cursor.execute(f"SELECT category, COUNT(*) as count, MAX(price) as max, MIN(price) as min , \
                               ROUND(SUM(price), 2) as sum, ROUND(AVG(price), 2) as avg FROM {table} GROUP BY category")

    write_file([dict(r) for r in response.fetchall()], 'out/4/2.json')


def get_quantity_by_category(cursor, table):
    response = cursor.execute(f"SELECT category, MAX(quantity) as max, MIN(quantity) as min , \
                               SUM(quantity) as sum, AVG(quantity) as avg FROM {table} GROUP BY category")

    write_file([dict(r) for r in response.fetchall()], 'out/4/3.json')


def get_data_from_city(cursor, table, city):
    response = cursor.execute(
        f"SELECT name, category, views, fromCity FROM {table} WHERE fromCity = ? GROUP BY fromCity",
        [city],
    )

    write_file([dict(r) for r in response.fetchall()], 'out/4/4.json')


global conn
global cursor
if __name__ == "__main__":
    try:
        conn, cursor = create_connection(dbname)

        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table} 
                           (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, price INTEGER, quantity INTEGER, 
                           category TEXT DEFAULT '', fromCity TEXT, isAvailable boolean, views INTEGER, counter INTEGER DEFAULT 0)
                        """)

        insert_table(cursor, products_data, table)
        conn.commit()

        update_database(cursor, table, update_data)
        conn.commit()

        get_top_updated(cursor, table)
        get_prices_by_category(cursor, table)
        get_quantity_by_category(cursor, table)
        get_data_from_city(cursor, table, 'Москва')

        print('Данные успешно записаны в директорию ./out/4')
    except Exception as exc:
        print(f'Exception: {exc}')
        print('Ошибка при работе с данными')
    finally:
        drop_table(cursor, table)
        conn.close()
