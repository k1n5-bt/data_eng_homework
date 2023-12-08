from common import create_connection, drop_table, insert_table, get_data, get_stat_data, get_freq, write_file
from json import load

dbname = "db/5/5.db"
table_personal = 'person'
table_location = 'location'
table_job = 'job'


def get_count_by_state(cursor):
    response = cursor.execute(f"SELECT state, COUNT(state) as count FROM {table_location} GROUP BY state ORDER BY count DESC")
    return [dict(r) for r in response.fetchall()]


def update_persons_status_by_state(cursor, state):
    cursor.execute(
        f"UPDATE {table_personal} SET status = 'active' WHERE id in (SELECT person_id FROM {table_location} WHERE state = ?)",
        [state]
    )


def get_persons_status_and_states(cursor):
    response = cursor.execute(
        f"SELECT {table_personal}.first_name, {table_location}.state, {table_personal}.status FROM {table_personal}, {table_location} "
        f"WHERE {table_personal}.id = {table_location}.person_id")
    return [dict(r) for r in response.fetchall()]


def get_data_from_count_cities_in_state(cursor):
    response = cursor.execute(
        f"SELECT state, count(city) as count FROM {table_location} GROUP BY state",
    )

    return [dict(r) for r in response.fetchall()]


global conn
global cursor
if __name__ == "__main__":
    try:
        conn, cursor = create_connection(dbname)

        drop_table(cursor, table_personal)
        drop_table(cursor, table_job)
        drop_table(cursor, table_location)

        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_personal} 
                           (id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT, first_name TEXT, middle_name TEXT,
                           last_name TEXT, username TEXT, password TEXT, email TEXT, phoneNumber TEXT)
                        """)

        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_location} 
                           (person_id REFERENCES {table_personal} (id), street TEXT, city TEXT, state TEXT,
                            country TEXT, zip INTEGER)
                        """)

        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_job} 
                           (person_id REFERENCES {table_personal} (id), title TEXT, descriptor TEXT, area TEXT,
                            type TEXT, company TEXT)
                        """)

        with open('data/5/data.json', 'r') as f:
            data = load(f)
            count = 1
            persons = []
            jobs = []
            locations = []
            for i in data['result']:
                personal_info = i['personal information']
                personal_info['id'] = count
                persons.append(personal_info)

                job_info = i['job']
                job_info['person_id'] = count
                jobs.append(job_info)

                loc_info = i['location']
                loc_info['person_id'] = count
                locations.append(loc_info)

                count += 1

            insert_table(cursor, persons, table_personal)
            conn.commit()

            insert_table(cursor, jobs, table_job)
            conn.commit()

            insert_table(cursor, locations, table_location)
            conn.commit()

        get_data(cursor, table_personal, 'out/5/1.json', limit=100, sort_by='first_name')

        write_file(get_count_by_state(cursor), 'out/5/2.json')

        update_persons_status_by_state(cursor, 'New Mexico')

        write_file(get_persons_status_and_states(cursor), 'out/5/3.json')

        write_file(get_data_from_count_cities_in_state(cursor), 'out/5/4.json')

        print('Данные успешно записаны в директорию ./out/5')
    except Exception as exc:
        print(f'Exception: {exc}')
        print('Ошибка при работе с данными')
    finally:
        drop_table(cursor, table_job)
        drop_table(cursor, table_location)
        drop_table(cursor, table_personal)
        conn.close()
