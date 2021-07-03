from training_raw_data_validation.raw_validation import RawDataValidation
from DataTransform_Training.DataTransformation import DataTransform
from DataTypeValidation_Insertion_Training.DataTypeValidation import dbOperation
from app_logging import App_logger
from datetime import datetime

class train_validation:

    def __init__(self, path):
        self.raw_data = RawDataValidation(path)
        self.dataTransform = DataTransform()
        self.db_operation = dbOperation()
        self.log_writer = App_logger()
        self.file_object = open("Training_Logs/Training_Main_Log.txt", "a+")

    def train_validation(self):
        try:
            self.log_writer.log(self.file_object, "Start of Validation of files for prediction")

            # extraction values from predictionn schema
            LengthOfDateSampleInFile, LengthOfTimeSampleInFile, column_name, no_of_columns = self.raw_data.value_from_schema()

            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()

            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateSampleInFile, LengthOfTimeSampleInFile)

            # validation column length
            self.raw_data.validateColumnLength(no_of_columns)



        except Exception as e:
            raise e
