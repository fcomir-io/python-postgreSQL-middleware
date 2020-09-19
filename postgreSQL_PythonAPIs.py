import psycopg2
from configparser import ConfigParser
import os
from pathlib import Path


class PostgreSQL_PythonAPIs:
    def __init__(self, _filename):
        self.db_initFile_Name = _filename
        
    def __config(self):
        # Create a parser
        parser = ConfigParser()        
        CONTENT__DATABASE_INFO__PATH = str(Path(self.db_initFile_Name))
        ### print("Path:", CONTENT__DATABASE_INFO__PATH)
        # Read config file
        parser.read(CONTENT__DATABASE_INFO__PATH)       
        # Get section, default to postgresql
        db_params = {}
        section = 'postgresql'
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db_params[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))

        return db_params

    def ConnectPostgreSQL(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:            
            # read connection parameters            
            db_params = self.__config()

            ###print("db_params:", db_params)

            if db_params != {}:
                # connect to the PostgreSQL server
                print('Connecting to the PostgreSQL database...')
                conn = psycopg2.connect(**db_params)
            
                # create a cursor
                cur = conn.cursor()
            
                # execute a statement                
                cur.execute('SELECT version()')
                db_version = cur.fetchone()

                # display the PostgreSQL database server version                
                print(' --> PostgreSQL database version:', db_version)
                print()
                
        except (Exception, psycopg2.DatabaseError) as error:
            return [-1, error]

        finally:     
            # return connection
            return [0, conn]

    def InsertDataIntoTable(self, db_connection, sql_command, valuesToInsert):        
        item_id = None
        try:
            # create a new cursor
            cur = db_connection.cursor()
            # execute the INSERT statement
            cur.execute(sql_command, valuesToInsert)
            # get the generated id back
            item_id = cur.fetchone()[0]
            # commit the changes to the database
            db_connection.commit()
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        return item_id

    def DeleteAllDataOfTable(self, db_connection, table_name):
        rows_deleted = None
        try:
            # create a new cursor
            cur = db_connection.cursor()
            # execute the SELECT statement
            cur.execute("DELETE FROM " + table_name)
            # get the number of updated rows
            rows_deleted = cur.rowcount
            # commit the changes to the database
            db_connection.commit()
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        return rows_deleted

            