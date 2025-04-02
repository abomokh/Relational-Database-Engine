import mysql.connector
import data_insertion
import database_initialization



def retrieve_table(cursor, table_name):
    try:
        # Execute the SELECT query
        cursor.execute(f"SELECT * FROM {table_name}")
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]
        
        # Print the column names
        print(f"Table: {table_name}")
        print(" | ".join(column_names))
        print("-" * 50)
        
        # Print each row
        for row in rows:
            print(" | ".join(str(value) for value in row))
            
    except mysql.connector.Error as err:
        print(f"Error retrieving table {table_name}: {err}")



if __name__ == "__main__":
    # Connect to the database
    con = mysql.connector.connect(
        host="localhost",
        user="mohamedj",
        port= 3305,
        password="moh5969",
        database="mohamedj",
    )

    cursor = con.cursor()

    #retrieve_table(cursor, "Movie")  # Replace "Movie" with your table name
    # Create tables
    database_initialization.create_tables(con, cursor)

    # Insert data into tables
    data_insertion.insert_data(con, cursor)
    
    # Query the database

    
    
    if con.is_connected():
        cursor.close()
        con.close()
        print("MySQL connection closed.")
    
