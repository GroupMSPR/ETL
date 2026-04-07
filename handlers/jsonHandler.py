import json
import os

import pandas
from fileManager import MoveToError, WriteLog
from config import TO_IMPORT_PATH

def convertJsonToPanda(file):
    file_path = os.path.join(TO_IMPORT_PATH, file)

    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        if isinstance(data, dict):
            df = pandas.DataFrame(data["data"])
        elif isinstance(data, list):
            df = pandas.DataFrame(data)
        else:
            WriteLog(file, "fileType not understood. If object, it must contain a 'data' list.")
            MoveToError(file)
            return None

        return df

    except Exception as ex:
        WriteLog(file, str(ex))
        MoveToError(file)
        return None
