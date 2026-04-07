from typing import Any

import pandas

from sqlalchemy.orm import Session
from config import Exercise, Food, Health_metric, User
from utils.fileManager import MoveToArchive, MoveToError, WriteLog
from utils.dataframeFormatter import formatDataFrame

def sendToTable(data: pandas.DataFrame, file: str, session: Session):

    fileLowered = file.lower()

    if "user" in fileLowered :
        sendUserToDb(data, file ,session)
    elif "exercise" in fileLowered :
        sendExerciseToDb(data, file, session)
    elif "food" in fileLowered :
        sendFoodToDb(data, file, session)
    elif "health" in fileLowered :
        sendHealthMetricToDb(data, file, session)
    else :
        WriteLog(file, "no matches with a table")
        MoveToError(file)

def sendUserToDb(data: pandas.DataFrame, file: str, session: Session):
    succesful : bool = True

    errorMessage = ""
    field = [
        "email",
        "password",
        "first_name",
        "last_name",
        "birthdate",
        "gender",
        "weight",
        "height",
        "body_fat_pct",
        "constraints",
        "physical_activity_level",
        "daily_caloric_intake",
        "goal",
        "subscription",
        "date_subscription"
    ]

    data, errorMessage = formatDataFrame(data, field)
    if errorMessage != "":
        WriteLog(file, errorMessage)
        MoveToError(file)
        return

    # data = data.fillna(0)

    try: 
        if "birthdate" in data:
            data["birthdate"] = pandas.to_datetime(data["birthdate"]).dt.date
    
        if "subscription_date" in data:
            data["subscription_date"] = pandas.to_datetime(data["subscription_date"]).dt.date

        errorMessage = ""

        for index,row in data.iterrows():

            user : User = User()
            
            placeHolderMessage = ""

            user.email ,placeHolderMessage = addData(row, "email")
            errorMessage += placeHolderMessage

            user.password ,placeHolderMessage = addData(row, "password")
            errorMessage += placeHolderMessage
            
            user.first_name ,placeHolderMessage = addData(row, "first_name")
            errorMessage += placeHolderMessage

            user.last_name ,placeHolderMessage = addData(row, "last_name")
            errorMessage += placeHolderMessage

            user.birthdate ,placeHolderMessage = addData(row, "birthdate")
            errorMessage += placeHolderMessage

            gender = row.get("gender").lower()
            if "gender" in row and gender in ['male', 'female', 'other']:
                user.gender = row.get("gender")
            else :
                errorMessage += "file does not contain gender attribute or gender is misspelled or invalid.\n"

            user.weight ,placeHolderMessage = addData(row, "weight")
            errorMessage += placeHolderMessage

            user.height ,placeHolderMessage = addData(row, "height")
            errorMessage += placeHolderMessage

            if user.weight is not None and user.weight is not None:
                user.bmi = round(user.weight / (user.height**2), 2) 

            user.body_fat_pct ,placeHolderMessage = addData(row, "body_fat_pct")
            errorMessage += placeHolderMessage

            constraints = row.get("constraints") 
            if (isinstance(constraints, list)):
                user.constraints = ", ".join(constraints)
            elif ("constraints" in row) :
                user.constraints = row.get("constraints") or "Non renseigné"
            else :
                user.constraints = "Non renseigné"

            physicalActivityLevel = row.get("physical_activity_level")
            if "physical_activity_level" in row and physicalActivityLevel in ['Sedentary', 'Moderate', 'Active']:
                user.physical_activity_level = row.get("physical_activity_level")
            else :
                errorMessage += "file does not contain physical_activity_level attribute or physical_activity_level is misspelled or invalid.\n"

            user.daily_caloric_intake ,placeHolderMessage = addData(row, "daily_caloric_intake")
            errorMessage += placeHolderMessage

            user.goal ,placeHolderMessage = addData(row, "goal")
            if placeHolderMessage != "":
                user.goal = "Non renseigné"

            subscription = row.get("subscription")
            if "subscription" in row and subscription in ['Freemium', 'Premium', 'Premium+']:
                user.subscription = subscription
            else :
                errorMessage += "file does not contain subscription attribute or subscription is misspelled or invalid.\n"
            
            if "date_subscription" in row and pandas.notna(row.get("date_subscription")) :
                user.date_subscription = row.get("date_subscription")

            session.add(user)
        if errorMessage != "":
            succesful = False
            WriteLog(file, errorMessage)

        if succesful:
            session.commit() 
            
    except Exception as ex:
        succesful = False
        session.rollback()
        WriteLog(file, str(ex))
    
    if (succesful):
        MoveToArchive(file)
    else :
        MoveToError(file)

def sendExerciseToDb(data: pandas.DataFrame, file: str, session: Session):
    succesful : bool = True

    errorMessage = ""
    field = [
        "name",
        "difficulty_level",
        "type",
        "target_muscle",
        "secondary_muscle",
        "equipment",
        "instructions",
        "constraints"
    ]

    data, errorMessage = formatDataFrame(data, field)
    if errorMessage != "":
        WriteLog(file, errorMessage)
        MoveToError(file)
        return

    errorMessage = ""
    try:
        for index,row in data.iterrows():
            exercise: Exercise = Exercise()

            exercise.daily_caloric_intake ,placeHolderMessage = addData(row, "daily_caloric_intake")
            errorMessage += placeHolderMessage

            if "name" in row and row.get("name") != 0:
                exercise.name = row.get("name")
            else :
                succesful = False
                WriteLog(file, "file does not contain name attribute or name is misspelled or invalid.")
                break

            exercise.difficulty_level   = row.get("difficulty_level") or "Non renseigné"
            exercise.type               = row.get("type") or "Non renseigné"

            target_muscle = row.get("target_muscle") 
            if (isinstance(target_muscle, list)):
                exercise.target_muscle  = ", ".join(target_muscle)
            elif ("target_muscle" in row) :
                exercise.target_muscle  = row.get("target_muscle") or "Non renseigné"
            else :
                succesful = False
                WriteLog(file, "file does not contain name_exercice attribute or name_exercice is misspelled.")
                break

            secondaryMuscle = row.get("secondary_muscle") or "No Secondary Muscle"
            if (isinstance(secondaryMuscle, list)) :
                exercise.secondary_muscle  = ", ".join(row.get("secondary_muscle") or [])
            elif ("secondary_muscle" in row) :
                exercise.secondary_muscle  = secondaryMuscle

            equipment = row.get("equipment") or "No Equipment"
            if (isinstance(equipment, list)) :
                exercise.equipment  = ", ".join(row.get("equipment") or [])
            
            exercise.equipment  = equipment

            exercise.difficulty_level   = row.get("difficulty_level") or "Non renseigné"
            exercise.instructions       = row.get("instructions") or "Non renseigné"

            constraints = row.get("constraints") 
            if (isinstance(constraints, list)):
                exercise.constraints = ", ".join(constraints)
            elif ("constraints" in row) :
                exercise.constraints = row.get("constraints") or "Non renseigné"
            else :
                exercise.constraints = "Non renseigné"
                
            session.add(exercise)
        if (succesful):
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

def sendFoodToDb(data: pandas.DataFrame, file: str, session: Session) :
    succesful : bool = True

    errorMessage = ""
    field = [
        "name_food",
        "category",
        "calories",
        "protein",
        "carbohydrates",
        "fat",
        "fiber",
        "sugars",
        "sodium",
        "cholestorol"
    ]

    data, errorMessage = formatDataFrame(data, field)
    if errorMessage != "":
        WriteLog(file, errorMessage)
        MoveToError(file)
        return

    data = data.fillna(0)
    try: 
        for index,row in data.iterrows():

            food: Food = Food()

            if "name_food" in row and row.get("name_food") != 0:
                food.name = row.get("name_food")
            else :
                succesful = False
                WriteLog(file, "file does not contain name_food attribute or name_food is misspelled.")
                break

            food.category = row.get("category") or "Non renseigné"
            food.calories = row.get("calories") 
            food.protein = row.get("protein") 
            food.carbohydrates = row.get("carbohydrates") 
            food.fat = row.get("fat")
            food.fiber = row.get("fiber")
            food.sugars = row.get("sugars")

            sodium = row.get("sodium")  
            if sodium > 32767:
                WriteLog(file, food.name + " cholesterol is above smallint limit")
                succesful = False
                break
            else : 
                food.sodium = sodium

            cholestorol = row.get("cholestorol") 
            if cholestorol > 32767:
                WriteLog(file, food.name + " cholesterol is above smallint limit")
                succesful = False
                break
            else :
                food.cholestorol = cholestorol

            session.add(food)
        if succesful:
            session.commit()     
    except Exception as ex:
        succesful = False
        session.rollback()
        WriteLog(file, str(ex))
        
    
    if (succesful):
        MoveToArchive(file)
    else :
        MoveToError(file)

def sendHealthMetricToDb(data: pandas.DataFrame, file: str, session: Session):
    succesful : bool = True

    errorMessage = ""
    field = [
        "user_email",
        "date",
        "start_weight",
        "current_weight",
        "avg_bpm",
        "max_bpm",
        "resting_bpm",
        "steps_count",
        "sleep_time",
        "calories_burned",
        "active_minute",
        "workout_type"
    ]

    data, errorMessage = formatDataFrame(data, field)
    if errorMessage != "":
        WriteLog(file, errorMessage)
        MoveToError(file)
        return

    data = data.fillna(0)
    if "date" in data:
        data["date"] = pandas.to_datetime(data["date"])

    users = session.query(User.user_id, User.email).all()

    user_map = {}

    for user in users:
        user_map[user.email] = user.user_id

    try: 
        for index,row in data.iterrows():
            healthMetric : Health_metric = Health_metric()

            email = row.get("user_email")
            if "user_email" in row and email != 0:
                if email in user_map:
                    healthMetric.user_id = user_map[email]
                else :
                    succesful = False
                    WriteLog(file, "no user with that email")
                    break 
            else :
                succesful = False
                WriteLog(file, "file does not contain user_email attribute or user_email is misspelled.")
                break

            if "date" in row and row.get("date") != 0:
                healthMetric.date_ = row.get("date")
            else :
                succesful = False
                WriteLog(file, "file does not contain date attribute or date is misspelled.")
                break

            if "start_weight" in row and row.get("start_weight") != 0:
                healthMetric.start_weight = row.get("start_weight")
            else:
                succesful = False
                WriteLog(file, "file does not contain start_weight attribute or start_weight is misspelled.")
                break

            if "current_weight" in row and row.get("current_weight") != 0:
                healthMetric.current_weight = row.get("current_weight")
            else:
                succesful = False
                WriteLog(file, "file does not contain current_weight attribute or current_weight is misspelled.")
                break

            if "avg_bpm" in row and row.get("avg_bpm") != 0:
                healthMetric.avg_bpm = row.get("avg_bpm")
            else:
                succesful = False
                WriteLog(file, "file does not contain avg_bpm attribute or avg_bpm is misspelled.")
                break

            if "max_bpm" in row and row.get("max_bpm") != 0:
                healthMetric.max_bpm = row.get("max_bpm")
            else:
                succesful = False
                WriteLog(file, "file does not contain max_bpm attribute or max_bpm is misspelled.")
                break

            if "resting_bpm" in row and row.get("resting_bpm") != 0:
                healthMetric.resting_bpm = row.get("resting_bpm")
            else:
                succesful = False
                WriteLog(file, "file does not contain resting_bpm attribute or resting_bpm is misspelled.")
                break

            if "steps_count" in row and row.get("steps_count") != 0:
                healthMetric.steps_count = row.get("steps_count")
            else:
                succesful = False
                WriteLog(file, "file does not contain steps_count attribute or steps_count is misspelled.")
                break

            if "sleep_time" in row and row.get("sleep_time") != 0:
                healthMetric.sleep_time = row.get("sleep_time")
            else:
                succesful = False
                WriteLog(file, "file does not contain sleep_time attribute or sleep_time is misspelled.")
                break

            if "calories_burned" in row and row.get("calories_burned") != 0:
                healthMetric.calories_burned = row.get("calories_burned")
            else:
                succesful = False
                WriteLog(file, "file does not contain calories_burned attribute or calories_burned is misspelled.")
                break

            if "active_minute" in row and row.get("active_minute") != 0:
                healthMetric.active_minute = row.get("active_minute")
            else:
                succesful = False
                WriteLog(file, "file does not contain active_minute attribute or active_minute is misspelled.")
                break

            if "workout_type" in row and row.get("workout_type") != 0:
                healthMetric.workout_type = row.get("workout_type")
            else:
                succesful = False
                WriteLog(file, "file does not contain workout_type attribute or workout_type is misspelled.")
                break

            session.add(healthMetric)
        if succesful:
            session.commit()     
    except Exception as ex:
        succesful = False
        session.rollback()
        WriteLog(file, str(ex))
    
    if (succesful):
        MoveToArchive(file)
    else :
        MoveToError(file)

def addData(row, columnToCheck : str):
    if columnToCheck in row and pandas.notna(row.get(columnToCheck)):
        return row.get(columnToCheck), "" 
    else :
        return None, f"file does not contain {columnToCheck} attribute or {columnToCheck} is misspelled or invalid.\n"