import pandas

from sqlalchemy.orm import Session
from config import Exercise, Food, Health_metric, User
from fileManager import MoveToArchive, MoveToError, WriteLog

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
    data = data.fillna(0)

    try: 
        if "birthdate" in data:
            data["birthdate"] = pandas.to_datetime(data["birthdate"]).dt.date
    
        if "subscription_date" in data:
            data["subscription_date"] = pandas.to_datetime(data["subscription_date"]).dt.date

        for index,row in data.iterrows():

            user : User = User()

            if "email" in row and row.get("email") != 0:
                user.email = row.get("email")
            else :
                succesful = False
                WriteLog(file, "file does not contain email attribute or email is misspelled or invalid.")
                break

            if "password" in row and row.get("password") != 0:
                user.password = row.get("password")
            else :
                succesful = False
                WriteLog(file, "file does not contain password attribute or password is misspelled or invalid.")
                break
            
            if "first_name" in row and row.get("first_name") != 0:
                user.first_name = row.get("first_name")
            else :
                succesful = False
                WriteLog(file, "file does not contain first_name attribute or first_name is misspelled or invalid.")
                break

            if "last_name" in row and row.get("last_name") != 0:
                user.last_name = row.get("last_name")
            else :
                succesful = False
                WriteLog(file, "file does not contain last_name attribute or last_name is misspelled or invalid.")
                break 

            if "birthdate" in row and row.get("birthdate") != 0:
                user.birthdate = row.get("birthdate")
            else :
                succesful = False
                WriteLog(file, "file does not contain birthdate attribute or birthdate is misspelled or invalid.")
                break

            gender = row.get("gender").lower()
            if "gender" in row and gender in ['male', 'female', 'other']:
                user.gender = row.get("gender")
            else :
                succesful = False
                WriteLog(file, "file does not contain gender attribute or gender is misspelled or invalid.")
                break

            if "weight" in row and row.get("weight") != 0:
                user.weight = row.get("weight")
            else :
                succesful = False
                WriteLog(file, "file does not contain weight attribute or weight is misspelled or invalid.")
                break
            
            if "height" in row and row.get("height") != 0:
                user.height = row.get("height")
            else :
                succesful = False
                WriteLog(file, "file does not contain height attribute or height is misspelled or invalid.")
                break

            user.bmi = round(user.weight / (user.height**2), 2) 

            if "body_fat_pct" in row and row.get("body_fat_pct") != 0:
                user.body_fat_pct = row.get("body_fat_pct")
            else :
                succesful = False
                WriteLog(file, "file does not contain body_fat_pct attribute or body_fat_pct is misspelled or invalid.")
                break


            disease_type = row.get("disease_type") 
            if (isinstance(disease_type, list)):
                user.disease_type = ", ".join(disease_type)
            elif ("disease_type" in row) :
                user.disease_type = row.get("disease_type") or "Non renseigné"
            else :
                user.disease_type = "Non renseigné"

            severity = row.get("severity") 
            if (isinstance(severity, list)):
                user.severity  = ", ".join(severity)
            elif ("severity" in row) :
                user.severity  = row.get("severity") or "Non renseigné"
            else :
                user.severity = "Non renseigné"

            physicalActivityLevel = row.get("physical_activity_level")
            if "physical_activity_level" in row and physicalActivityLevel in ['Sedentary', 'Moderate', 'Active']:
                user.physical_activity_level = row.get("physical_activity_level")
            else :
                succesful = False
                WriteLog(file, "file does not contain physical_activity_level attribute or physical_activity_level is misspelled or invalid.")
                break

            if "daily_caloric_intake" in row and row.get("daily_caloric_intake") != 0:
                user.daily_caloric_intake = row.get("daily_caloric_intake")
            else :
                succesful = False
                WriteLog(file, "file does not contain daily_caloric_intake attribute or daily_caloric_intake is misspelled or invalid.")
                break

            if "goal" in row and row.get("goal") != 0:
                user.goal = row.get("goal")
            else :
                user.goal = "Non renseigné"

            subscription = row.get("subscription")
            if "subscription" in row and subscription in ['Freemium', 'Premium', 'Premium+']:
                user.subscription = subscription
            else :
                succesful = False
                WriteLog(file, "file does not contain subscription attribute or subscription is misspelled or invalid.")
                break
            
            if row.get("date_subscription") != 0:
                user.date_subscription = row.get("date_subscription")

            
            session.add(user)
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
    data = data.fillna(0)

    try:
        for index,row in data.iterrows():
            exercise: Exercise = Exercise()

            if "name_exercise" in row and row.get("name_exercise") != 0:
                exercise.name = row.get("name_exercise")
            else :
                succesful = False
                WriteLog(file, "file does not contain name_exercise attribute or name_exercise is misspelled or invalid.")
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