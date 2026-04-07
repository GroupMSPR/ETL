import datetime
import mimetypes
import os
import shutil

import magic

from config import ARCHIVE_PATH, ERROR_PATH, LOG_PATH, TO_IMPORT_PATH


def GetFileType(fileName : str) :
    mime , _ = mimetypes.guess_type(fileName)
    fileType : str = magic.from_file(fileName, mime=True)
    if (fileType == 'text/plain'):
        return mime.split("/")[1]
    else :
        return fileType.split("/")[1]

def MoveToArchive(fileName : str) :
    try :
        date : datetime = datetime.datetime.now().strftime("%Y_%d_%w_%H_%M_%S_")
        shutil.move(src=os.path.join(TO_IMPORT_PATH, fileName), dst=os.path.join(ARCHIVE_PATH, date + fileName))
    except Exception as ex:
        print(ex)

def MoveToError(fileName: str) :
    try :
        shutil.move(src=os.path.join(TO_IMPORT_PATH, fileName), dst=os.path.join(ERROR_PATH, fileName))
    except Exception as ex:
        print(ex)

def WriteLog(file : str, message : str) :
                date : datetime = datetime.datetime.now().strftime("%Y_%d_%w_%H_%M_%S")
                filename = os.path.splitext(file)[0]
                log_path = os.path.join(LOG_PATH, f"{date}_{filename}.log")
                with open(log_path, "a") as log:
                      log.write(message + "\n")