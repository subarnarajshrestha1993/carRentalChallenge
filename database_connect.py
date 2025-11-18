import mysql.connector
from config_reader import read_config
from pprint import pprint
import json

def get_db_conn():
    try:
        config_data = read_config()
        # Establish the connection
        conn = mysql.connector.connect(
            host=config_data['db_host'],      # Or the IP address/hostname of your MySQL server
            user=config_data['db_user'],  # Your MySQL username
            password=config_data['db_password'], # Your MySQL password
            database=config_data['database'] # The name of the database you want to connect to
        )

        print("Connection to MySQL database successful!")
        return conn


    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(f"Error: {err}")


def execute_query(conn, query):
    mycursor =None
    try:
        # You can now create a cursor object to execute SQL queries
        mycursor = conn.cursor()

        # Example: Execute a query (e.g., show tables)
        mycursor.execute(query)

        if "insert" in query.lower():
            print("\nData inserted")
            conn.commit()
        else:
            columns = [i[0]  for i in mycursor.description]
            return(columns,  mycursor.fetchall())
        # for table in mycursor:
        #     print(table)

    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(f"Error: {err}")

    finally:
        if 'mycursor' in locals() and mycursor:
            mycursor.close()


# if __name__ == "__main__":
#     conn = get_db_conn()
#     select_query = "select * from new_users"
#     # query = "insert into users(id, name, status,) values(1, 'Ram', 'Active')"
#     columns, records = execute_query(conn, select_query)
#     print(columns)
#     pprint(records)
#     data_list = []
#     for record in records:
#         ele = dict(zip(columns, record))
#         data_list.append(ele)
#
#     pprint(data_list)
#     print(type(data_list))
#     for row in data_list:
#         if "json_record" in row:
#             json_record = json.loads(row['json_record'])
#             record_id = json_record['id']
#             record_name = json_record['name']
#             record_status = json_record.get('status')
#             record_modified_time= json_record['modified_time']
#         id = row['unique_id']
#         name = row['ip_address']
#         user_status = 'Active'
#         modified_time = row['modified_time']
#         insert_query = (f"insert into transformed_users(id, name, status, modified_time, record_id, record_name, record_status, record_modified_time) "
#                         f"values({id}, '{name}','{user_status}', '{modified_time}', {record_id}, '{record_name}', '{record_status}', '{record_modified_time}' )")
#         print(insert_query)
#         execute_query(conn, insert_query)
#     if conn:
#         conn.close()