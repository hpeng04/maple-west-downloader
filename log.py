from datetime import datetime, timedelta
import pandas as pd
import os

class Log:
    path = r'Logs/'
    missing_path = r'failed_downloads.txt'
    yesterday = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

    @staticmethod
    def write(message, date=yesterday):
        if not os.path.exists(Log.path):
            os.makedirs(Log.path)
        path = Log.path + date + '.txt'
        with open(path, 'a') as file:
            file.write(message + '\n')
    
    @staticmethod
    def get_path(date=yesterday):
        path = Log.path + date + '.txt'
        return path
    
    @staticmethod
    def record_failed_downloads(unit_no, date, url):
        datatype = "Minute"
        if "Monthly" in url:
            datatype = "Hour"
        with open(Log.missing_path, 'a') as file:
            file.write(f'{unit_no}, {datatype}, {url}\n')
        return