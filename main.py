import os

from dotenv import load_dotenv

import pandas
from handlers.csvHandler import convertCsvToPanda
from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.orm import Session
from config import TO_IMPORT_PATH, Base
from utils.fileManager import GetFileType
from handlers.dbHandler import sendToTable
from handlers.jsonHandler import convertJsonToPanda

def Main() :

    load_dotenv()

    try:
        db_url = os.getenv("DATABASE_URL")

        if not db_url:
            print("Erreur : DATABASE_URL est vide ou non définie.")
            return

        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)

        engine: Engine = create_engine(db_url)
        Base.metadata.create_all(engine)
        session: Session = Session(engine)
        print("connection to db succeded")

    except Exception as e:
        print("connection to db failed")
        exit()

    filesNames : list[str] = os.listdir(TO_IMPORT_PATH)
    filesNames = sorted(filesNames) # if there is more then 9 table we'll have to redo it to sort based on value

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