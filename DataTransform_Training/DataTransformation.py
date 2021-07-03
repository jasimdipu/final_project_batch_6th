from os import listdir
from app_logging.App_logger import AppLogger
import pandas as pd

class DataTransform:

    def __init__(self):
        self.goodDataPath = "Training_Raw_Files_Validated/Good_Raw"
        self.logger = AppLogger()

    def addQuotesToStringValuesInColumn(self):

        log_file = open("Training_Logs/addQuotesToStringValuesInColumn.txt", 'a+')
        try:
            only_file = [f for f in listdir(self.goodDataPath)]
            for file in only_file:
                data = pd.read_csv(self.goodDataPath+'/'+file)
                data['Date'] = data['Date'].apply(lambda x: "'"+str(x)+"'")
                data.to_csv(self.goodDataPath+'/'+file, index=None, header=True)
                self.logger.log(log_file, "%s: Quotes added successfully!!"%file)
        except Exception as e:
            self.logger.log(log_file, "Data transformation failed because: %s" % e)
            log_file.close()
        log_file.close()