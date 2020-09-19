#!/usr/bin/python
# Insert parent folder to be able to add postgreSQL_PythonAPIs
import sys
sys.path.insert(0, './')
sys.path.insert(0, '../')
sys.path.insert(0, 'middleware/')
from postgreSQL_PythonAPIs import PostgreSQL_PythonAPIs
import os
from pathlib import Path

if __name__ == '__main__':

    # Check where is the execution from
    currentPath = Path(os.getcwd())
    print("Running from path: ", currentPath)

    baseDirectory = str(currentPath)
    while (baseDirectory.endswith('/iot-modbus-solution') != True):
        # Go up two levels from the current directory
        currentPath = currentPath.parent
        baseDirectory = str(currentPath)

    print("Base Directory: ", baseDirectory)
    CONTENT__DATABASE_INFO__PATH = baseDirectory + "/__content/database.ini"
    
    # Object of type PythonAPI to deal with psycopg2
    pythonConnector = PostgreSQL_PythonAPIs(CONTENT__DATABASE_INFO__PATH)
    
    # Start connection process
    result = pythonConnector.ConnectPostgreSQL()
    if result[0] == 0:
        db_connection = result[1]
    else:
        print(result[1])
        db_connection = None

    # Check if connection was ok
    if db_connection != None:        
        # Empty Data table
        result = pythonConnector.DeleteAllDataOfTable(db_connection, "data")        
        print(" ==> In table 'data' deleted:", result, "rows!")
        print()

        # Close connection with DB
        if db_connection is not None:
            db_connection.close()
            print('Database connection closed.')
            print()
