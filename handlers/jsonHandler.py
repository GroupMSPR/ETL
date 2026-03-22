import json
import os

import pandas
from fileManager import MoveToError, WriteLog
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

"""
    figures out which table the file is based on the first row that start with "name" about and send it to the database and archive or error it

    file: string = file that need to be imported without path
    no return
"""
# def SendJsonToDb(file: str, session: Session) :
    
#     data = json.load(open(os.path.join(TO_IMPORT_PATH, file)))
#     if isinstance(data, dict):
#         df = pandas.DataFrame(data["data"])
#     elif isinstance(data, list):
#         df = pandas.DataFrame(data)
#     else :
#         WriteLog(file, "fileType not understood if it is an object make sure your data has a data attribute that is a list of your thing or just have a list")
#         MoveToError(file)
#         return
#     attributeNameList: pandas.Index[str] = df.columns
    
#     nameName = ""
#     for attributeName in attributeNameList:
#         if(attributeName.startswith("name")):
#             nameName = attributeName
#             break
    
#     match nameName:
#         case "nameUser":
#             print()
#         case "nameExercise":
#             JsonExercise(df, file, session)
#         case "nameFood":
#             JsonFood(df, file, session)
#         case _:
#             WriteLog(file, "no matches with a table")
#             MoveToError(file)
