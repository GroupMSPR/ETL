import datetime
import json
import logging
import mimetypes
import os
import shutil
import magic
import psycopg2
from sqlalchemy import NUMERIC, SMALLINT, TEXT, TIMESTAMP, Column, Numeric, SmallInteger, String, Integer, Text, Time
from sqlalchemy.orm import DeclarativeBase
import pandas

from config import LOG_PATH, TO_IMPORT_PATH
from fileManager import GetFileType
from handlers.jsonHandler import SendJsonToDb

def Main() :
    try:
        conn : psycopg2 = psycopg2.connect(
            dbname="mspr",
            user="postgres",
            password="azerty",
            host="localhost",
            port="5434"
        )
        cur = conn.cursor()

        print("connection to db succeded")

    except Exception as e:

        print("connection to db failed")
        exit

    filesNames : list[str] = os.listdir(TO_IMPORT_PATH)

    for file in filesNames:
        match GetFileType(os.path.join(TO_IMPORT_PATH, file)):
            case "xml":
                print()
            case "csv":
                print()
            case "json":
                SendJsonToDb(file)
            case _:
                print("FileType Not supported")

    if conn:
        conn.close()
        print("Database connection closed.")

"""
create logger instance

file :str = name of the file without path
"""
def getLogger(file: str) -> logging.Logger:

    date : datetime = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_")
    filename = os.path.splitext(file)[0]
    log_path = os.path.join(LOG_PATH, f"{date}_{filename}.log")
    
    logger = logging.getLogger(filename)
    logger.setLevel(logging.INFO)
    
    handler = logging.FileHandler(log_path)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
    logger.addHandler(handler)
    return logger



Main()