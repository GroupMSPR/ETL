import os

import pandas
from handlers.csvHandler import convertCsvToPanda
from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.orm import Session
from config import TO_IMPORT_PATH, Base
from fileManager import GetFileType
from handlers.dbHandler import sendToTable
from handlers.jsonHandler import convertJsonToPanda


def Main() :
    try:
        engine: Engine = create_engine('')
        Base.metadata.create_all(engine)
        session: Session = Session(engine)
        print("connection to db succeded")

    except Exception as e:
        print("connection to db failed")
        exit()

    filesNames : list[str] = os.listdir(TO_IMPORT_PATH)

    for file in filesNames:
        data : pandas.DataFrame
        match GetFileType(os.path.join(TO_IMPORT_PATH, file)):
            case "csv" | "xlsx":
                data = convertCsvToPanda(file)
            case "json":
                data = convertJsonToPanda(file)
            case _:
                print("FileType Not supported")
                continue
        if data is not None:
            sendToTable(data, file, session)

    if session:
        session.close()
        print("Database connection closed.")

Main()