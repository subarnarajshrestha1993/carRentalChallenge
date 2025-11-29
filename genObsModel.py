from config_reader import read_config
from pprint import pprint
import json
from pg_connect import get_connection,execute_query


#business_date = 2025-11-26

def transform():
    conn = None
    try:
        conn = get_connection()
        select_query = "select * from kafka.TransformedTeslaInfo"
        columns, records = execute_query(conn, select_query)
        print(columns)
        pprint(records)
        data_list = []
        for record in records:
            ele = dict(zip(columns, record))
            data_list.append(ele)

        pprint(data_list)
        print(type(data_list))
        model_name = None
        for row in data_list:
            # pprint(row)
            car_uid = row['car_uid']
            generic_model = row['genericmodel']
            if generic_model:
                generic_model = json.loads((generic_model).replace('\'', '"'))
                first_ele = generic_model[0]
                pprint(first_ele)
                # pprint(json_record)
                uid = first_ele['uid']
                #TeslaUUID = json_record['TeslaUUID']
                name = first_ele.get('name')
                site = first_ele.get('site')
                # model = json.loads(json_record['model'])
                count = first_ele['count']
                notes = first_ele.get('notes')


                insert_query1 = f"""insert into kafka.ev_generic_model(uid, name,site,count,notes, car_uid)
                values('{uid}','{name}','{site}','{count}','{notes}', {car_uid})
                ON CONFLICT(uid)
                DO UPDATE SET
                name = EXCLUDED.name,
                site = EXCLUDED.site,
                count = EXCLUDED.count,
                notes = EXCLUDED.notes;
                """

                print(insert_query1)
                execute_query(conn, insert_query1)

    except Exception as e:
        if conn:
            conn.close()
        print(f"exception occured: {e}")


if __name__ == "__main__"  :
    transform()