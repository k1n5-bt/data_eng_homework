import json
import csv
from common import write_file, create_connection, insert_data

db = "data_eng_1"

with open('data/1/task_1_item.json', mode='r', encoding="utf-8") as f:
    data = json.load(f)

with open('data/2/task_2_item.csv', 'r') as f:
    item = csv.DictReader(f, delimiter=';')
    for i in item:
        data.append(i)


def get_stat_by_salary(conn):
    query = [
        {
            "$group": {
                "_id": "result",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"},
            }
        }
    ]
    return [*conn.aggregate(query)]


def get_count_by_job(conn):
    query = [
        {
            "$group": {
                "_id": "$job",
                "count": {"$sum": 1},
            }
        },
        {
            "$sort": {
                "count": -1
            }
        }
    ]
    return [*conn.aggregate(query)]


def get_stat_by_params(conn, column, field):
    query = [
        {
            "$group": {
                "_id": f"${column}",
                "max": {"$max": f"${field}"},
                "min": {"$min": f"${field}"},
                "avg": {"$avg": f"${field}"},
            }
        }
    ]

    return [*conn.aggregate(query)]


def get_salary_by_age(conn, match_age, level_age='max', level_salary='max'):
    query = [
        {
            "$match": {
                "age": match_age,
            }
        },
        {
            "$group": {
                "_id": "result",
                "min_age": {f"${level_age}": "$age"},
                "max_salary": {f"${level_salary}": "$salary"}
            }
        }
    ]

    return [*conn.aggregate(query)]


def get_age_by_city_by_salary(conn):
    query = [
        {
            "$match":
                {"salary": {"$gt": 50000}}
        },
        {
            "$group": {
                "_id": "$city",
                "max": {"$max": "$age"},
                "min": {"$min": "$age"},
                "avg": {"$avg": "$age"},
            }
        },
        {
            "$sort": {
                "avg": -1
            }
        }
    ]

    return [*conn.aggregate(query)]


def get_stat_salary_by_city_job_age(conn):
    query = [
        {
            "$match":
                {
                    "city": {"$in": ["Кордова", "Ереван", "Лас-Росас", "Сараево"]},
                    "job": {"$in": ["Психолог", "Косметолог", "Учитель"]},
                    "$or": [
                        {"age": {"$gt": 18, "$lt": 25}},
                        {"age": {"$gt": 50, "$lt": 65}}
                    ]
                }
        },
        {
            "$group": {
                "_id": "result",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"},
            }
        },
    ]

    return [*conn.aggregate(query)]


def get_sorted_cities_by_job_salary(collection, jobs, min_salary, max_salary):
    a = [
        {
            "$match":
                {
                    "job": {"$in": jobs},
                    "salary": {"$gt": min_salary, "$lt": max_salary},
                }
        },
        {
            "$group": {
                "_id": "$city",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"},
            }
        },
        {
            "$sort": {
                "max": 1
            }
        }
    ]

    return [*collection.aggregate(a)]


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

        write_file(get_stat_by_salary(conn), 'out/2/1.json')
        write_file(get_count_by_job(conn), 'out/2/2.json')

        write_file(get_stat_by_params(conn, "city", "salary"), 'out/2/3.json')
        write_file(get_stat_by_params(conn, "job", "salary"), 'out/2/4.json')
        write_file(get_stat_by_params(conn, "city", "age"), 'out/2/5.json')
        write_file(get_stat_by_params(conn, "job", "age"), 'out/2/6.json')

        write_file(get_salary_by_age(conn, 18, level_salary='max', level_age='min'), 'out/2/7.json')
        write_file(get_salary_by_age(conn, 65, level_salary='min', level_age='max'), 'out/2/8.json')

        write_file(get_age_by_city_by_salary(conn), 'out/2/9.json')
        write_file(get_stat_salary_by_city_job_age(conn), 'out/2/10.json')

        write_file(get_sorted_cities_by_job_salary(conn, ['Косметолог', 'Водитель', 'Психолог'], min_salary=50000, max_salary=150000), 'out/2/11.json')

    except Exception as err:
        print(str(err))
    finally:
        columns = db.list_collection_names()
        [db.drop_collection(i) for i in columns]
