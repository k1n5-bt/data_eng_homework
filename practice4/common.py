import json
from sqlite3 import Row, connect


def write_file(data, filename):
    with open(filename, mode='w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def create_connection(file):
    connection = connect(file)
    connection.row_factory = Row
    cursor = connection.cursor()
    return connection, cursor


def drop_table(conn, table):
    conn.execute(f'DROP TABLE {table}')


def insert_table(conn, data, table):

    for value in data:
        columns = ', '.join(value.keys())
        values_for_req = ', '.join('?' * len(value.values()))
        sql_request = f'INSERT INTO {table} ({columns}) VALUES ({values_for_req})'

        values = [x for x in value.values()]
        conn.execute(sql_request, values)


def get_data(
        conn,
        table,
        filename,
        sort_direct='DESC',
        sort_by=None,
        limit=None,
        filtered_field=None,
        filtered_value=None,
):
    query = f"SELECT * FROM {table}"
    if filtered_field and filtered_value:
        query += f" WHERE {filtered_field} > {filtered_value}"
    elif filtered_field or filtered_value:
        raise Exception('filtered_field and filtered_value must use both')
    if sort_by:
        query += f" ORDER BY {sort_by} {sort_direct}"
    if limit:
        query += f" LIMIT {limit}"
    result = conn.execute(query)
    write_file([dict(r) for r in result.fetchall()], filename)


def get_stat_data(conn, table, filename, field):
    result = conn.execute(
        f"SELECT MIN({field}) as min, "
        f"MAX({field}) as max, "
        f"SUM({field}) as sum, "
        f"COUNT({field}) as count, "
        f"AVG({field}) as avg from {table}")
    write_file(dict(result.fetchone()), filename)


def get_freq(conn, table, filename, field):
    result = conn.execute(f"SELECT {field}, COUNT(*) as count from {table} GROUP BY {field}")
    write_file([dict(r) for r in result.fetchall()], filename)
