import json
import os

import pandas
from sqlalchemy import Connection
from sqlalchemy.orm import Session
from fileManager import MoveToArchive, MoveToError, WriteLog
from config import TO_IMPORT_PATH, Exercise, Food

"""
    figures out which table the file is based on the first row that start with "name" about and send it to the database and archive or error it

    file: string = file that need to be imported without path
    no return
"""
def SendJsonToDb(file: str, session: Session) :
    
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
            JsonExercise(df, file, session)
        case "nameFood":
            print()
        case "name":
            WriteLog(file, "no matches with a table")

def JsonExercise(data: pandas.DataFrame, file: str, session: Session):
    for index,row in data.iterrows():
        try:
            exercise: Exercise = Exercise()
            succesful : bool = True
            if "nameExercise" in row:
                exercise.name = row["nameExercise"]
            else :
                succesful = False
                WriteLog(file, "file does not contain name_exercice attribute or name_exercice is misspelled.")
                break

            exercise.difficulty_level   = row.get("difficulty_level", "")
            exercise.type               = row.get("type", "")

            target_muscle = row.get("target_muscle") 
            if (isinstance(target_muscle, list)):
                exercise.target_muscle  = ", ".join(target_muscle)
            elif ("target_muscle" in row) :
                exercise.target_muscle  = row.get("target_muscle", "")
            else :
                succesful = False
                WriteLog(file, "file does not contain name_exercice attribute or name_exercice is misspelled.")
                break

            secondaryMuscle = row.get("secondary_muscle", "no secondary muscle")
            if (isinstance(secondaryMuscle, list)):
                exercise.secondary_muscle  = ", ".join(row.get("secondary_muscle", []))
            elif ("secondary_muscle" in row) :
                exercise.secondary_muscle  = row.get("secondary_muscle", "no secondary muscle")

            equipment = row.get("equipment", "no equipment")
            if (isinstance(equipment, list)):
                exercise.equipment  = ", ".join(row.get("equipment", []))
            
            exercise.equipment  = equipment

            exercise.difficulty_level   = row.get("difficulty_level", "")
            exercise.instructions       = row.get("instructions", "")
            session.add(exercise)
            session.commit()

        except Exception as ex:
            succesful = False
            session.rollback()
            WriteLog(file, str(ex))
            return ex
    if (succesful):
        MoveToArchive(file)
    else :
        MoveToError(file)

def JsonFood(data: pandas.DataFrame) : 
    food: Food = Food()
    food.name = data["name"]
    