'''
input: csv file for pushing to db
read csv --> push all to db

output: db data complete.

'''

import psycopg2
from psycopg2 import sql


# Database connection parameters
DB_HOST = '127.0.0.1'
DB_NAME = 'datalake'
DB_USER = 'superset'
DB_PASSWORD = 'superset'
# Function to insert data into PostgreSQL
def insert_data(table_name, data):
    # Establish the database connection
    connection = psycopg2.connect(
        host=DB_HOST, 
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

    # Create a cursor object
    cursor = connection.cursor()
    
    # Create the insert query dynamically
    columns = data.keys()
    values = [data[column] for column in columns]
    
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(values))
    )
    
    # Execute the insert query
    cursor.execute(insert_query, values)
    
    # Commit the transaction
    connection.commit()
    
    print("Data inserted successfully.")



    if cursor:
        cursor.close()
    if connection:
        connection.close()

# Example usage
if __name__ == "__main__":
    # Define the table name and data to insert
    table_name = 'your_table_name'
    data_to_insert = {
        'column1': 'value1',
        'column2': 'value2',
        'column3': 'value3'
    }
    
    # Call the function to insert data
    insert_data(table_name, data_to_insert)
