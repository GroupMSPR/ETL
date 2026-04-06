import json
import os

import pandas
from utils.fileManager import MoveToError, WriteLog
from config import TO_IMPORT_PATH

def convertJsonToPanda(file): 
    data = json.load(open(os.path.join(TO_IMPORT_PATH, file)))
    if isinstance(data, dict):
        df = pandas.DataFrame(data["data"])
    elif isinstance(data, list):
        df = pandas.DataFrame(data)
    else :
        WriteLog(file, "fileType not understood if it is an object make sure your data has a data attribute that is a list of your thing or just have a list")
        MoveToError(file)
        return None
    return df