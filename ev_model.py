from config_reader import read_config
from pprint import pprint
import json
from pg_connect import get_connection,execute_query


business_date = 2025-11-26

def transform():
    conn = None
    try:
        conn = get_connection()
        select_query = "select * from kafka.TeslaInfo where event_ts::date = CURRENT_DATE -7;"
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
            # string_prop = row['string_properties']
            if "json_record" in row:
                json_record = json.loads((row['json_record']).replace('\'', '"'))
                # pprint(json_record)
                car_uid = json_record['car_uid']
                TeslaUUID = json_record['TeslaUUID']
                name = json_record.get('name')
                classification = json_record.get('classification')
                # model = json.loads(json_record['model'])
                model = json_record['model']
                model_carUid = model.get('car_uid')
                model_TeslaUUID = model.get('TeslaUUID')
                model_name = model.get('name')
                model_classification = json_record.get('classification')
                mobiles = json_record['mobiles']
                print(mobiles)
                if mobiles:
                    if len(mobiles)  == 2:
                        mobile1 = mobiles[0]
                        mobile2 = mobiles[1]
                    elif len(mobiles) == 1:
                        mobile1 = mobiles[0]
                        mobile2 = None

                else:
                    mobile1 = None
                    mobile2 = None

                geoLocation = json_record.get('geoLoc')
                if geoLocation:
                    elev= geoLocation.get('elev')
                    geoLoc_s95 = geoLocation.get('s95')
                    exactLoc= geoLocation.get('exactLoc')
                    coordinates = exactLoc['coordinates']
                    x_coordinates = coordinates[0]
                    y_coordinates = coordinates[1]
                else:
                    x_coordinates = None
                    y_coordinates = None
                    elev= None
                    geoLoc_s95 = None


                print(f"(X,Y) : ({x_coordinates}, {y_coordinates})")
                # record_modified_time= json_record['modified_time']
            #id = row['car_uid']
            #TeslaUUID = row['TeslaUUID']
            #user_status = 'Active'
            #modified_time = row['modified_time']
            insert_query1 = f"""insert into kafka.car_info_dim(car_uid, name)
            values({car_uid},'{name}' )
            ON CONFLICT(car_uid)
            DO UPDATE SET
            name = EXCLUDED.name;
            """

            #
            # insert_query2 = f"""insert into kafka.model_info_dim(car_uid,model_carUid, model_name)
            #     values({car_uid},{model_carUid}, '{model_name}' )
            #     ON CONFLICT(model_carUid, car_uid)
            #     DO UPDATE SET
            #     model_name = EXCLUDED.model_name
            # """
            #
            # insert_query3 = (
            #     f"insert into kafka.orders_fact(car_uid, TeslaUUID, classification,model_carUid,  model_TeslaUUID, model_classification) "
            #     f"values({car_uid}, '{TeslaUUID}', '{classification}', {model_carUid}, '{model_TeslaUUID}', '{model_classification}')")


            if model_name  == "jeff bezos":
                # mobile_description  = f"mobile info for {model_name}"
                insert_query1 = f"""insert into kafka.ev_jb_model(model_name, car_uid, model_classification, mobile1, mobile2)
                     values('{model_name}',
                    {car_uid}, '{model_classification}','{mobile1}', '{mobile2}')
                     ON CONFLICT(model_name)
                        DO UPDATE SET
                        model_classification = EXCLUDED.model_classification,
                        mobile1 = EXCLUDED.mobile1,
                        mobile2 = EXCLUDED.mobile2
                        ;
                    """
            elif model_name == "jeff bezos1":
                # mobile_description  = f"mobile info for tesla model y: {model_name}"
                insert_query1 = f"""insert into kafka.ev_jb1_model(model_name,car_uid, model_classification, mobile1, mobile2) 
                values('{model_name}',
                    {car_uid},'{model_classification}','{mobile1}', '{mobile2}')
                      ON CONFLICT(model_name)
                        DO UPDATE SET
                        model_classification = EXCLUDED.model_classification,
                        mobile1 = EXCLUDED.mobile1,
                        mobile2 = EXCLUDED.mobile2
                        ;
                    """
            else:
            #     mobile_description  = f"model info for {model_name}"
                insert_query1 = f"""insert into kafka.ev_other_model(model_name,car_uid, model_classification, mobile1, mobile2) 
                    values('{model_name}',
                    {car_uid},{model_classification}','{mobile1}', '{mobile2}')
                     ON CONFLICT(model_name)
                        DO UPDATE SET
                        model_classification = EXCLUDED.model_classification,
                        mobile1 = EXCLUDED.mobile1,
                        mobile2 = EXCLUDED.mobile2
                        ;
                    """


            print(insert_query1)
            #print(insert_query2)
            #print(insert_query3)
            #print(insert_query1)

            execute_query(conn, insert_query1)
            #execute_query(conn, insert_query2)
            #execute_query(conn, insert_query3)
            #execute_query(conn, insert_query1)
    except Exception as e:
        if conn:
            conn.close()
        print(f"exception occured: {e}")


if __name__ == "__main__"  :
    transform()