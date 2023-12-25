import json
from common import write_file, create_connection, insert_data

db = "data_eng_1"

with open('data/1/task_1_item.json', mode='r', encoding="utf-8") as f:
    data = json.load(f)


def get_data_by_salary(conn):
    result = conn.find({}, limit=10).sort({'salary': -1})
    write_file([*result], 'out/1/1.json')


def get_sorted_and_filtered(conn):
    result = conn.find({'age': {"$lt": 30}}, limit=15).sort({'salary': -1})
    write_file([*result], 'out/1/2.json')


def get_sorted_and_filtered_by_predict(conn, city, jobs):
    result = conn.find({'city': city, "job": {"$in": jobs}}, limit=10).sort({'age': 1})
    write_file([*result], 'out/1/3.json')


def get_count_by_filter(conn):
    result = conn.find(
        {
            "age": {"$gt": 25, "$lt": 35}, "year": {"$gte": 2019, "$lte": 2022},
            "$or": [
                {"salary": {"$gt": 50000, "$lte": 75000}},
                {"salary": {"$gt": 125000, "$lt": 150000}}
            ]
        }
    )
    result_list = [*result]

    write_file(result_list, 'out/1/4.json')
    write_file(len(result_list), 'out/1/5.json')


if __name__ == "__main__":
    try:
        db = create_connection(db)
        conn = db.person

        for i in data:
            i['salary'] = int(i['salary'])
            i['year'] = int(i['year'])
            i['id'] = int(i['id'])
            i['age'] = int(i['age'])

        insert_data(conn, data)

        get_data_by_salary(conn)
        get_sorted_and_filtered(conn)
        get_sorted_and_filtered_by_predict(conn, "Бургос", ["Психолог", "Программист", "Архитектор"])
        get_count_by_filter(conn)

    except Exception as err:
        print(str(err))
    finally:
        columns = db.list_collection_names()
        [db.drop_collection(i) for i in columns]
