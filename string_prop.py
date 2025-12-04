from config_reader import read_config
from pprint import pprint
import json
from pg_connect import get_connection,execute_query


business_date = 2025-11-26

def transform():
    conn = None
    try:
        conn = get_connection()
        select_query = "select * from kafka.amazon_info;"
        columns, records = execute_query(conn, select_query)
        print(columns)
        # pprint(records)
        data_list = []
        for record in records:
            ele = dict(zip(columns, record))
            data_list.append(ele)

        # pprint(data_list)
        print(len(data_list))
        model_name = None
        for row in data_list:
            # pprint(row)
            # string_prop = row['string_properties']
            if "amazon_json_record" in row:
                json_record = json.loads((row['amazon_json_record']).replace('\'', '"'))
                # pprint(json_record)
                coi_groups = json_record['coiGroups']
                # pprint(coi_groups)
                for item in coi_groups:
                    temp_table_name = (item.get('user_name')).split('.')[-1]
                    temp_table_name = temp_table_name.replace(' ', '_')
                    dim_table = f"dim_{temp_table_name}"
                    fact_table = f"fact_{temp_table_name}"
                    print(f"fact table: {fact_table}   dim table : {dim_table}")
                    user_id = item.get('user_uid')
                    user_name =  item.get('user_name')
                    dim_insert_qry  = f"insert into kafka.{dim_table}(user_uid, user_name) values('{user_id}', '{user_name}');"
                    print(dim_insert_qry)
                    execute_query(conn, dim_insert_qry)
                    order_props = json_record['OrderProperties']
                    # pprint(order_props)
                    all_value = None
                    fun_value = None
                    for inner_item  in order_props:
                        col_name = inner_item.get('user_name')
                        if col_name == 'AllegraRREE':
                            all_value = inner_item.get('string_value')
                        elif col_name == 'FunGames1' :
                            fun_value = inner_item.get('string_value')
                        else:
                            continue

                        print(f"all value : {all_value} and fun value : {fun_value}")
                    id = item.get('tu_uid')
                    class_var = item.get('class')
                    fact_insert_query = (f"insert into kafka.{fact_table}(id, class, AllegraRREE, FunGames1, user_id)  values('{id}', '{class_var}',"
                                         f"'{all_value}', '{fun_value}' , '{user_id}');")
                    print(fact_insert_query)
                    execute_query(conn, fact_insert_query)

    except Exception as e:
        if conn:
            conn.close()
        print(f"exception occured: {e}")


if __name__ == "__main__"  :
    transform()