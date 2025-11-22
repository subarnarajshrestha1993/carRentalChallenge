from config_reader import read_config
from pprint import pprint
import json
from pg_connect import get_connection,execute_query


def transform():
    conn = None
    try:
        conn = get_connection()
        select_query = "select * from kafka.TeslaInfo"
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
            # pprint(row)
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
            # #modified_time = row['modified_time']
            insert_query = (f"insert into kafka.TransformedTeslaInfo(car_uid, TeslaUUID, name, classification,model_carUid,  model_TeslaUUID, model_name, model_classification,"
                            f" mobile1, mobile2,geoloc_elev,geoloc_s95,x_coordinates,y_coordinates) "
                            f"values({car_uid}, '{TeslaUUID}','{name}', '{classification}', {model_carUid}, '{model_TeslaUUID}', '{model_name}', '{model_classification}', "
                            f"'{mobile1}', '{mobile2}','{elev}','{geoLoc_s95}','{x_coordinates}','{y_coordinates}' )")
            print(insert_query)
            execute_query(conn, insert_query)
    except Exception as e:
        if conn:
            conn.close()
        print(f"exception occured: {e}")


if __name__ == "__main__"  :
    transform()