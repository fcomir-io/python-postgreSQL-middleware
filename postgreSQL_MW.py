import psycopg2
import json
import os
from pathlib import Path
from middleware.postgreSQL_PythonAPIs import PostgreSQL_PythonAPIs

class PostgreSQL_Middleware:

    def __init__(self, db_initFile_Name, configFile_Path):

        # Object of type PythonAPI to deal with psycopg2
        self.pythonConnector = PostgreSQL_PythonAPIs(db_initFile_Name)
        
        # Connect to DB specified in database.ini
        self.db_connection = self.__connectToDatabase()
        if (self.db_connection != None):
            # Create tables if there are not already created
            self.tableSetup_Status = False  # not yet setup
            self.result = self.__setup_Tables(configFile_Path)
        else:
            print("Aborting!")
            self.result = -99

    """ PRIVATE FUNCTIONS FOR INITIALIZATION """
    def __connectToDatabase(self):
        """ Connect to the PostgreSQL database server """                
        # start connection process
        result = self.pythonConnector.ConnectPostgreSQL()
        ###print("Result:", result)
        if result[0] == 0:
            return result[1]
        else:
            print(result[1])
            return None

    def __setup_Tables(self, configFile_Path):
        # Get configuraiton from file        
        if configFile_Path.is_file():
            # Check if file is JSON ok
            try:
                # Since we re working with python v3.5, open needs a string as argument
                json_object = json.loads(open(str(configFile_Path)).read())
            except Exception as e:
                return 2
        else:
            return 2
        
        # Check if class has not yet initialized tables
        if (self.tableSetup_Status != True):
            try:
                if self.db_connection is not None:
                    cur = self.db_connection.cursor()
                    
                    # create table one by one
                    for table in json_object["tables"]:
                        #print(table["sql_command"])
                        try:
                            cur.execute(table["sql_command"])
                        except (Exception, psycopg2.DatabaseError) as error:
                            print(' [ERROR] ==> ', error)
                            self.db_connection.rollback()
                    
                    # close communication with the PostgreSQL database server
                    cur.close()

                    # commit the changes
                    self.db_connection.commit()
                else:
                    print("No connection with data base")

            except (Exception, psycopg2.DatabaseError) as error:
                print(error)       
            
            finally:
                # Update flag
                self.tableSetup_Status = True
                return 0                
        else:
            print("Tables were already created")
            return 3
        

    """ PUBLIC FUNCTIONS """
    def GetConfigurationStatus(self):
        return self.result

    def InsertDataIntoTable(self, timestamp, value):
        # Necesito
        # - Tabla
        # - Columnas a insertar
        # - Valores a introducir en las tablas
        #   --- Q pasa si no tengo valores suficientes o en exceso?
        # Devuelve 0 o error
                # Print out information

        # Insert Test Bench
        sql_command = """INSERT INTO data(timestamp, value, device_id, signal_id) 
	                 VALUES (%s, %s, %s, %s)
	                 RETURNING entry_id;"""
        valuesToInsert = [timestamp, value, 1, 1]
        newItem_id = result = self.pythonConnector.InsertDataIntoTable(self.db_connection, sql_command, valuesToInsert)
        print("newItem_id: ", newItem_id)

    def DeleteAllDataOfTable(self, tableName):
        # Empty Data table
        result = self.pythonConnector.DeleteAllDataOfTable(self.db_connection, tableName)        
        print(" ==> In table", tableName, "deleted:", result, "rows!")
        print()

    def Query_Data(Self):
        pass