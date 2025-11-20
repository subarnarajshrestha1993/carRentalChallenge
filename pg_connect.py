import psycopg2

from dotenv import load_dotenv
import os

base_dir = os.path.dirname(__file__)
# print(base_dir)
env_file_path = os.path.join(base_dir, 'config', 'dotenv.env')
# print(env_file_path)
load_dotenv(dotenv_path=env_file_path)

db_host = os.getenv('db_host')
db_port = os.getenv('db_port')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
database =os.getenv('database')

def get_connection():
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=database,
            user=db_user,
            password=db_password
        )
        print("Connected successfully")
        return conn



    except Exception as e:
        print("Error:", e)
        return None

def execute_query(conn, query):
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        if "select" in query.lower():
            columns = [i[0]  for i in cursor.description]
            return (columns, cursor.fetchall())
        else:
            print("insert completed")
            conn.commit()

    except Exception as e:
        print(f"Exception occured: {e}")
    finally:
        if cursor:
            cursor.close()


# if __name__ == '__main__':
#     conn = None
#     try:
#         conn = get_connection()
#         cursor = conn.cursor()
#         cursor.execute("SELECT * from kafka.TeslaInfo;")
#         print(cursor.fetchone())
#     except Exception as e:
#         print(f"Exception occured: {e}")
#     finally:
#         if conn:
#             conn.close()

