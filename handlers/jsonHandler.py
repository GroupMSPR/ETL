import json
import os

import pandas

from fileManager import MoveToArchive, MoveToError, WriteLog
from config import TO_IMPORT_PATH, Exercise, Food

"""
    figures out which table the file is based on the first row that start with "name" about and send it to the database and archive or error it

    file: string = file that need to be imported without path
    no return
"""
def SendJsonToDb(file: str) :
    
    data = json.load(open(os.path.join(TO_IMPORT_PATH, file)))
    df = pandas.DataFrame(data["data"])
    
    attributeNameList: pandas.Index[str] = df.columns
    
    nameName = ""
    for attributeName in attributeNameList:
        if(attributeName.startswith("name")):
            nameName = attributeName
            break
    
    match nameName:
        case "nameUser":
            print()
        case "nameExercise":
            JsonExercise(df, file)
        case "nameFood":
            print()
        case "name":
            JsonExercise(df, file)
    

def JsonExercise(data: pandas.DataFrame, file: str):
    exercise: Exercise = Exercise()
    for index,row in data.iterrows():
        try:
            succesfull : bool = True
            if "name" in row:
                exercise.name = row["name"]
            else :
                succesfull = False
                WriteLog(file, "file does not contain name_exercice attribute or name_exercice is misspelled.")
                break

            exercise.difficulty_level   = row.get("difficulty_level", "")
            exercise.type               = row.get("type", "")

            if (row.get("target_muscle") is [str]):
                exercise.target_muscle  = ", ".join(row.get("target_muscle", ""))
            elif ("target_muscle" in row) :
                exercise.target_muscle  = row.get("target_muscle", "")
            else :
                succesfull = False
                WriteLog(file, "file does not contain name_exercice attribute or name_exercice is misspelled.")
                break

            if (row.get("secondary_muscle") is [str]):
                exercise.secondary_muscle  = ", ".join(row.get("secondary_muscle", ""))
            elif ("secondary_muscle" in row) :
                exercise.secondary_muscle  = row.get("secondary_muscle", "no secondary muscle")

            if (row.get("equipment") is [str]):
                exercise.equipment  = ", ".join(row.get("equipment", ""))
            elif ("equipment" in row) :
                exercise.equipment  = row.get("equipment", "no secondary muscle")

            exercise.difficulty_level   = row.get("difficulty_level", "")
            exercise.instructions       = row.get("instructions", "")
            
        except Exception as ex:
            WriteLog(file, ex)
            return ex
    if (succesfull):
        MoveToArchive(file)
    else :
        MoveToError(file)

def JsonFood(data: pandas.DataFrame) : 
    food: Food = Food()
    food.name = data["name"]
    