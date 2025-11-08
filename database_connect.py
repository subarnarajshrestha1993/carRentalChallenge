import mysql.connector


def get_db_conn():
    try:
        # Establish the connection
        conn = mysql.connector.connect(
            host="localhost",      # Or the IP address/hostname of your MySQL server
            user="subarna",  # Your MySQL username
            password="subarna", # Your MySQL password
            database="StudentInformation" # The name of the database you want to connect to
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


def insert_record(conn, query):
    mycursor =None
    try:
        # You can now create a cursor object to execute SQL queries
        mycursor = conn.cursor()

        # Example: Execute a query (e.g., show tables)
        mycursor.execute(query)

        print("\nData inserted")
        conn.commit()
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


if __name__ == "__main__":
    conn = get_db_conn()
    query = "insert into users(id, name, status) values(1, 'Ram', 'Active')"
    insert_record(conn, query)
    if conn:
        conn.close()