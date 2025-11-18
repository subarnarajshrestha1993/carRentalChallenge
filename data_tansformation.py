from config_reader import read_config
from pprint import pprint
import json
from database_connect import execute_query, get_db_conn


def transform():
    conn = None
    try:
        conn = get_db_conn()
        select_query = "select * from new_users"
        # query = "insert into users(id, name, status,) values(1, 'Ram', 'Active')"
        columns, records = execute_query(conn, select_query)
        print(columns)
        pprint(records)
        data_list = []
        for record in records:
            ele = dict(zip(columns, record))
            data_list.append(ele)

        pprint(data_list)
        print(type(data_list))
        for row in data_list:
            if "json_record" in row:
                json_record = json.loads(row['json_record'])
                record_id = json_record['id']
                record_name = json_record['name']
                record_status = json_record.get('status')
                record_modified_time= json_record['modified_time']
            id = row['unique_id']
            name = row['ip_address']
            user_status = 'Active'
            modified_time = row['modified_time']
            insert_query = (f"insert into transformed_users(id, name, status, modified_time, record_id, record_name, record_status, record_modified_time) "
                            f"values({id}, '{name}','{user_status}', '{modified_time}', {record_id}, '{record_name}', '{record_status}', '{record_modified_time}' )")
            print(insert_query)
            execute_query(conn, insert_query)
    except Exception as e:
        if conn:
            conn.close()
        print(f"exception occured: {e}")


if __name__ == "__main__"  :
    transform()