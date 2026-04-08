import os
import pandas as pd

from config import TO_IMPORT_PATH
from utils.fileManager import WriteLog, MoveToError

def convertCsvToPanda(file: str):
    file_path = os.path.join(TO_IMPORT_PATH, file)

    try:
        df = pd.read_csv(file_path, sep=None, engine="python", encoding="utf-8" )   
        return df
    
    except Exception as ex:
        WriteLog(file, f"CSV read failed: {ex}")
        MoveToError(file)
        return None