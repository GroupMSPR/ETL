import os

from dotenv import load_dotenv

import pandas

from googleapiclient.discovery import Resource
from handlers.csvHandler import convertCsvToPanda
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session
from config import ERROR_ID, LOG_ID, TMP_PATH, TO_IMPORT_ID, Base
from utils import driveHelper
from utils.fileManager import GetFileType, WriteLog
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

    except Exception as e:
        print("DB error:", e)
        return
    
    service: Resource = driveHelper.get_drive_service()

    files = driveHelper.list_files(service, TO_IMPORT_ID)
    
    for file in files:

        file_id = file["id"]
        file_name = file["name"]

        driveHelper.download_file(service, file_id, os.path.join(TMP_PATH, file_name))
        local_path = os.path.join(TMP_PATH, file_name)

        data : pandas.DataFrame = None
        match GetFileType(local_path):
            case "csv" | "xlsx":
                data = convertCsvToPanda(local_path, file, service)
            case "json":
                data = convertJsonToPanda(local_path, file, service)
            case _:
                driveHelper.move_file(service, file_id, ERROR_ID)
                WriteLog(service, LOG_ID, file_name, "unrecognise dataType")
                continue

        if data is not None:
            sendToTable(data, file, session, service)

    if session:
        session.close()
        print("Database connection closed.")

Main()