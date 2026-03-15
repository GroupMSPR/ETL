import os

from sqlalchemy import Connection, Engine, create_engine
from sqlalchemy.orm import Session
from config import TO_IMPORT_PATH, Base
from fileManager import GetFileType
from handlers.jsonHandler import SendJsonToDb

def Main() :
    try:
        engine: Engine = create_engine('postgresql+psycopg2://postgres:azerty@localhost:5434/mspr')
        Base.metadata.create_all(engine)
        session: Session = Session(engine)
        print("connection to db succeded")

    except Exception as e:
        print("connection to db failed")
        exit()

    filesNames : list[str] = os.listdir(TO_IMPORT_PATH)

    for file in filesNames:
        match GetFileType(os.path.join(TO_IMPORT_PATH, file)):
            case "xml":
                print()
            case "csv":
                print()
            case "json":
                SendJsonToDb(file, session)
            case _:
                print("FileType Not supported")

    if session:
        session.close()
        print("Database connection closed.")

Main()