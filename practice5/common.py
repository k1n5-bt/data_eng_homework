from pymongo import MongoClient
import json


def create_connection(db):
    return MongoClient()[db]


def insert_data(conn, data):
    res = conn.insert_many(data)


def round_field(coll, field):
    data = coll.find()
    for i in data:
        i[field] = round(i[field], 2)

    coll.save(data)


def write_file(data, filename):
    with open(filename, mode='w', encoding='utf-8') as f:
        json.dump(data, f, default=str, ensure_ascii=False)
