import json
import csv
from common import create_connection, insert_data

db = "data_eng_1"

with open('data/1/task_1_item.json', mode='r', encoding="utf-8") as f:
    data = json.load(f)

with open('data/2/task_2_item.csv', 'r') as f:
    item = csv.DictReader(f, delimiter=';')
    for i in item:
        data.append(i)

with open('data/3/task_3_item.text', mode='r', encoding="utf-8") as f:
    lines = f.readlines()
    item = {}
    for i in lines:
        if '=====' in i:
            data.append(item)
            item = {}
        else:
            s = i.strip().split('::')
            item[s[0]] = s[1]


def drop_by_salary(conn):
    res = conn.delete_many(
        {
            "$or": [
                {"salary": {"$lt": 25_000}},
                {"salary": {"$gt": 175_000}},
            ]
        }
    )

    print(res)


def added_document_ages(conn):
    res = conn.update_many({}, {"$inc": {"age": 1}})
    print(res)


def added_salary_by_jobs(conn, jobs):
    res = conn.update_many(
        {
            "job": {
                "$in": jobs
            }
        },
        {
            "$mul": {
                "salary": 1.05
            }
        }
    )
    print(res)


def added_salary_by_cities(conn, cities):
    res = conn.update_many(
        {
            "city": {"$nin": cities}
        },
        {
            "$mul": {
                "salary": 1.07
            }
        }
    )
    print(res)


def added_salary_by_params(conn, cities, jobs, min_age, max_age):
    res = conn.update_many(
        {
            "city": {"$in": cities},
            "job": {"$nin": jobs},
            "age": {"$gte": min_age, "$lte": max_age}
        },
        {
            "$mul": {
                "salary": 1.1
            }
        },
    )
    print(res)


def drop_by_predicts(conn):
    res = conn.delete_many(
        {
            "$and": [
                {"age": {"$gte": 55}},
                {"salary": {"$lte": 200000}},
                {"city": {"$in": ['Москва', 'Авилес']}}
            ]
        }
    )
    print(res)


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

        drop_by_salary(conn)
        added_document_ages(conn)
        added_salary_by_jobs(conn, ['Менеджер', 'Продавец', 'Повар'])
        added_salary_by_cities(conn, ['Сан-Себастьян', 'Льейда', 'Таллин'])
        added_salary_by_params(conn, ['Махадаонда', 'Сория', 'Таллин'], ['Косметолог', 'Менеджер'], min_age=30, max_age=50)
        drop_by_predicts(conn)

    except Exception as err:
        print(str(err))
    finally:
        columns = db.list_collection_names()
        [db.drop_collection(i) for i in columns]
