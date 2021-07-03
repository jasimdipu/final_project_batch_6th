import shutil
import sqlite3
from os import listdir
import os
import csv
from app_logging.App_logger import AppLogger


class dbOperation:
    """
    This class shall be used for handling all the database/sql operation
    """

    def __init__(self):
        self.path = "Training_Database/"
        self.badFilePath = "Training_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_Files_Validated/Good_Raw"
        self.logger = AppLogger()

    def databaseConnection(self, DatabaseName):
        """
        database connection
        :param DatabaseName:
        :return:
        """

        try:
            conn = sqlite3.connect(self.path + DatabaseName + '.db')
            file = open("Training_Logs/DatabaseConnectionLog.txt", "a+")
            self.logger.log(file, "Database connected successfully %s" % DatabaseName)
            file.close()
        except ConnectionError as e:
            file = open("Training_Logs/DatabaseConnectionLog.txt", "a+")
            self.logger.log(file, "Error while connecting the database %s" % e)
            file.close()
            raise e

        return conn

    def createTableDB(self, DatabaseName, column_name):

        """
        create table in database
        :param Database:
        :param column_name:
        :return:
        """
        try:
            conn = self.databaseConnection(DatabaseName)
            c = conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'Good_Raw_Data'")
            if c.fetchone()[0] == 1:
                conn.close()
                file = open("Training_Logs/DBTableCreateLog.txt", "a+")
                self.logger.log(file, "Table created successfully")
                file.close()

                file = open("Training_Logs/DatabaseConnectionLog.txt", "a+")
                self.logger.log(file, "Database closed successfully" % DatabaseName)
                file.close()
            else:

                for key in column_name.keys():
                    type = column_name[key]

                    try:
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}"{dataType}'.format(column_name=key, dataType=type))
                    except:
                        conn.execute('CREATE TABLE Good_Raw_Data ("{column_name}"{dataType})'.format(column_name=key, dataType=type))
            conn.close()

            file = open("Training_Logs/DBTableCreateLog.txt", "a+")
            self.logger.log(file, "Table created successfully")
            file.close()

            file = open("Training_Logs/DatabaseConnectionLog.txt", "a+")
            self.logger.log(file, "Database closed successfully" % DatabaseName)
            file.close()
        except Exception as e:

            file = open("Training_Logs/DBTableCreateLog.txt", "a+")
            self.logger.log(file, "Error while creating table %s", e)
            file.close()
            conn.close()
            file = open("Training_Logs/DatabaseConnectionLog.txt", "a+")
            self.logger.log(file, "Database closed successfully" % DatabaseName)
            file.close()
            conn.close()
            raise e

    def selectingDataFromTableIntoCsv(self, Database):

        self.fileFromDb = "Training_File_From_DB"
        self.file_name = "InputFile.csv"
        sqlSelect = "SELECT * FROM Good_Raw_Data"
        log_file = open("Training_Logs/ExportToCsv.txt", "a+")

        try:
            cnn = self.databaseConnection(Database)
            cursor = cnn.cursor()
            cursor.execute(sqlSelect)

            result = cursor.fetchall()

            headers = [i for i in cursor.description]

            # make the CSV output directory

            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing
            csv_file = csv.writer(open(self.fileFromDb+self.file_name, 'w', newline=''), delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the csv file
            csv_file.writerow(headers)
            csv_file.writerows(result)

            self.logger.log(log_file, "File exported Successfully")
            log_file.close()
            cnn.close()
        except Exception as e:
            self.logger.log(log_file, "File exporting failed")
            log_file.close()


