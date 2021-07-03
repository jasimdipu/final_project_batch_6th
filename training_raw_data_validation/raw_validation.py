from app_logging.App_logger import AppLogger
import json
import os
import shutil
import datetime
from os import listdir
import re
import pandas as pd


class RawDataValidation:
    """This class shall be used for all the validation done by raw training data"""

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = "schema_training.json"
        self.logger = AppLogger()

    def value_from_schema(self):

        """
        This method extracts all the relevant information from the predefined 'schema flies'
        """

        try:
            with open(self.schema_path, "r"):
                dic = json.load()
            pattern = dic["SampleFileName"]
            length_of_data_sample_file = dic['LengthOfDataSampleFile']
            length_of_time_sample_file = dic['LengthOfTimeSampleFile']

            column_name = dic['ColumnName']
            number_of_column = dic['NumberOfColumn']

            file = open("Training_Logs/values_from_schema_validation_file.txt", "a+")
            message = "LengthOfDataSampleFile:: %s" % length_of_data_sample_file + "\t" + "LengthOfTimeSampleFile:: %s" % length_of_time_sample_file + "\t" + "NumberOfColumns:: %s" % number_of_column + "\n"
            self.logger.log(file, message)

            file.close()

        except ValueError as e:
            file = open("Training_Logs/values_from_schema_validation_file.txt", "a+")
            self.logger.log(file, "ValueError: Value not found inside schema_training.json")
            file.close()
            raise e
        except KeyError as e:
            file = open("Training_Logs/values_from_schema_validation_file.txt", "a+")
            self.logger.log(file, "KeyError: Key not found inside schema_training.json")
            file.close()
            raise e
        except Exception as e:
            file = open("Training_Logs/values_from_schema_validation_file.txt", "a+")
            self.logger.log(file, str(e))
            file.close()
            raise e
        return length_of_data_sample_file, length_of_time_sample_file, column_name, number_of_column

    def manualRegexCreation(self):
        regex = "['cement_strength']+['\_''[\_d][\_d]+\.csv"
        return regex

    def createDirectoryForGoodDataOrBadRawData(self):
        """
        create directory for good or bad raw data
        """

        try:
            path = os.path.join("Training_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", "a+")
            self.logger.log(file, "Error while creating directory %s" % e)
            file.close()
            raise OSError

    def delExistingGoodTrainingFolder(self):

        """
        This method delete the directory mode to store the good Data after loading the data in the table.
        """

        try:
            path = "Training_Raw_Files_Validated/"
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + "Good_Raw/")
                file = open("Training_Logs/GeneralLog.txt", "a+")
                self.logger.log(file, "Good raw data deleted successfully!!")
                file.close()
        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", "a+")
            self.logger.log(file, "Error while deleting directory %s" % e)
            file.close()

    def delExistingBadTrainingFolder(self):

        """
        This method delete the directory mode to store the bad Data after loading the data in the table.
        """

        try:
            path = "Training_Raw_Files_Validated/"
            if os.path.isdir(path + "Bad_Raw/"):
                shutil.rmtree(path + "Bad_Raw/")
                file = open("Training_Logs/GeneralLog.txt", "a+")
                self.logger.log(file, "Bad raw data deleted successfully!!")
                file.close()
        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", "a+")
            self.logger.log(file, "Error while deleting directory %s" % e)
            file.close()

    def moveBadDataToArchive(self):

        now = datetime.datetime.now()
        date = now.date()
        time = now.time()

        try:
            source = "Training_Raw_Files_Validated/Bad_Raw/"

            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                destination = "TrainingArchiveBadData/BadData" + str(date) + str(time)
                if not os.path.isdir(destination):
                    os.makedirs(destination)

                files = os.listdir(source)

                for f in files:
                    if f not in os.listdir(destination):
                        shutil.move(source + f, destination)

                file = open("Training_Logs/GeneralLog.txt", "a+")
                self.logger.log(file, "Bad file move to archive")
                file.close()

                path = "Training_Raw_Files_Validated/"

                if os.path.isdir(path + "Bad_Raw/"):
                    shutil.rmtree(path + "Bad_Raw/")
                self.logger.log(file, "Bad raw data deleted successfully!!")
                file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", "a+")
            self.logger.log(file, "Error while moving bad data files to archive %s" % e)
            file.close()

            raise e

    def validationFileNameRaw(self, regex, length_of_data_sample_file, length_of_time_sample_file):
        """
        delete the good or bad raw data incase last run was unsuccessfull and folders were not deleted

        good_raw.csv, bad_raw.csv
        :param regex:
        :param length_of_data_sample_file:
        :param length_of_time_sample_file:
        :return:
        """
        self.delExistingGoodTrainingFolder()
        self.delExistingBadTrainingFolder()

        only_files = [f for f in listdir(self.Batch_Directory)]
        try:
            self.createDirectoryForGoodDataOrBadRawData()
            f = open("Training_Logs/nameValidation.txt", "a+")

            for filename in only_files:
                if (re.match(regex, filename)):
                    split_at_dot = re.split(".csv", filename)
                    split_at_dot = (re.split("_", split_at_dot[0]))
                    if len(split_at_dot[2] == length_of_data_sample_file):
                        if len(split_at_dot[3] == length_of_data_sample_file):
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f, "Valid File name|| File moved to good raw folder %s", filename)
                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f, "Valid File name|| File moved to bad raw folder%s", filename)
                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f, "Valid File name|| File moved to bad raw folder%s", filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Valid File name|| File moved to bad raw folder%s", filename)

            f.close()
        except Exception as e:
            file = open("Training_Logs/nameValidationLog.txt", "a+")
            self.logger.log(file, "Error while validating file name %s" % e)
            file.close()

            raise e

    def validateColumnLength(self, numberOfColumns):
        try:
            f = open('Training_Logs/columnValidationLog.txt', 'a+')
            self.logger.log(f, "Column length validation started")
            for file in listdir("Training_Raw_Files_Validated/Good_Raw/"):
                csv = pd.read_csv("Training_Raw_Files_Validated/Good_Raw/" + file)
                if csv.shape[1] == numberOfColumns:
                    pass
                else:
                    shutil.move("Training_Raw_Files_Validated/Good_Raw/", "Training_Raw_Files_Validated/Bad_Raw/")
                    self.logger.log(f, "Invalid column length for the file!! File moved to the bad folder" % file)
                self.logger.log(f, "Length validation completed")
        except OSError as e:
            f = open("Training_Logs/columnValidationLog.txt", "a+")
            self.logger.log(f, "Error Occured while moving the file :: %s" % e)
            f.close()
            raise e
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", "a+")
            self.logger.log(f, "Error Occured while moving the file :: %s" % e)
            f.close()
            raise e


