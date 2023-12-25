from json import load
from common import write_file, create_connection, insert_data

db = "data_eng_2"

with open('data/4/data.json', 'r') as f:
    data = load(f)
    persons = []
    jobs = []
    locations = []
    for i in data['result']:
        persons.append(i['personal information'])
        jobs.append(i['job'])
        locations.append(i['location'])


def get_data_by_lastname(conn):
    result = conn.find({}, limit=10).sort({'last_name': -1})
    return [*result]


def get_sorted_and_filtered_by_predict(conn, states):
    result = conn.find({"state": {"$in": states}}, limit=10).sort({'zip': 1})
    return [*result]


def drop_by_predicts(conn):
    res = conn.delete_many(
        {
            "$and": [
                {"country": {"$in": ["Honduras"]}},
                {"state": {"$in": ["Vermont"]}}
            ]
        }
    )
    print(res)


def get_count_by_status(conn):
    query = [
        {
            "$group": {
                "_id": "$status",
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


def get_count_by_state(conn):
    query = [
        {
            "$group": {
                "_id": "$state",
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


if __name__ == "__main__":
    try:
        db = create_connection(db)
        conn = db.person

        insert_data(conn, persons)
        insert_data(conn, jobs)
        insert_data(conn, locations)

        write_file(get_data_by_lastname(conn), 'out/4/1.json')
        write_file(get_sorted_and_filtered_by_predict(conn, ['Virginia', 'Texas']), 'out/4/2.json')
        write_file(get_count_by_status(conn), 'out/4/3.json')
        write_file(get_count_by_state(conn), 'out/4/4.json')
        drop_by_predicts(conn)

    except Exception as err:
        print(str(err))
    finally:
        columns = db.list_collection_names()
        [db.drop_collection(i) for i in columns]
