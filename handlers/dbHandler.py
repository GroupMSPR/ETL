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
    if "birthdate" in data:
        data["birthdate"] = pandas.to_datetime(data["birthdate"]).dt.date
    
    if "subscription_date" in data:
        data["subscription_date"] = pandas.to_datetime(data["subscription_date"]).dt.date

    try: 
        for index,row in data.iterrows():

            user : User = User()

            if "email" in row and row["email"] != 0:
                user.email = row["email"]
            else :
                succesful = False
                WriteLog(file, "file does not contain email attribute or email is misspelled or invalid.")
                break

            if "password" in row and row["password"] != 0:
                user.password = row["password"]
            else :
                succesful = False
                WriteLog(file, "file does not contain password attribute or password is misspelled or invalid.")
                break
            
            if "first_name" in row and row["first_name"] != 0:
                user.first_name = row["first_name"]
            else :
                succesful = False
                WriteLog(file, "file does not contain first_name attribute or first_name is misspelled or invalid.")
                break

            if "last_name" in row and row["last_name"] != 0:
                user.last_name = row["last_name"]
            else :
                succesful = False
                WriteLog(file, "file does not contain last_name attribute or last_name is misspelled or invalid.")
                break 

            if "birthdate" in row and row["birthdate"] != 0:
                user.birthdate = row["birthdate"]
            else :
                succesful = False
                WriteLog(file, "file does not contain birthdate attribute or birthdate is misspelled or invalid.")
                break

            gender = row["gender"].lower()
            if "gender" in row and gender in ['male', 'female', 'other']:
                row["gender"]
                user.gender = row["gender"]
            else :
                succesful = False
                WriteLog(file, "file does not contain gender attribute or gender is misspelled or invalid.")
                break

            if "weight" in row and row["weight"] != 0:
                user.weight = row["weight"]
            else :
                succesful = False
                WriteLog(file, "file does not contain weight attribute or weight is misspelled or invalid.")
                break
            
            if "height" in row and row["height"] != 0:
                user.height = row["height"]
            else :
                succesful = False
                WriteLog(file, "file does not contain height attribute or height is misspelled or invalid.")
                break

            user.bmi = round(user.weight / (user.height^2)) 

            if "body_fat_pct" in row and row["body_fat_pct"] != 0:
                user.body_fat_pct = row["body_fat_pct"]
            else :
                succesful = False
                WriteLog(file, "file does not contain body_fat_pct attribute or body_fat_pct is misspelled or invalid.")
                break

            user.disease_type = row.get("disease_type") 
            user.severity = row.get("severity") 

            physicalActivityLevel = row["physical_activity_level"]
            if "physical_activity_level" in row and physicalActivityLevel in ['Sedentary', 'Moderate', 'Active']:
                row["physical_activity_level"]
                user.physical_activity_level = row["physical_activity_level"]
            else :
                succesful = False
                WriteLog(file, "file does not contain physical_activity_level attribute or physical_activity_level is misspelled or invalid.")
                break

            if "daily_caloric_intake" in row and row["daily_caloric_intake"] != 0:
                user.daily_caloric_intake = row["daily_caloric_intake"]
            else :
                succesful = False
                WriteLog(file, "file does not contain daily_caloric_intake attribute or daily_caloric_intake is misspelled or invalid.")
                break

            if "goal" in row and row["goal"] != 0:
                user.goal = row["goal"]
            else :
                succesful = False
                WriteLog(file, "file does not contain goal attribute or goal is misspelled or invalid.")
                break

            subscription = row["subscription"]
            if "subscription" in row and physicalActivityLevel in ['Freemium', 'Premium', 'Premium+']:
                row["subscription"]
                user.subscription = row["subscription"]
            else :
                succesful = False
                WriteLog(file, "file does not contain subscription attribute or subscription is misspelled or invalid.")
                break

            user.subscriptionDate = row["subscription_date"]

            
            session.add(User)
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

            if "name_exercise" in row and row["name_exercise"] != 0:
                exercise.name = row["name_exercise"]
            else :
                succesful = False
                WriteLog(file, "file does not contain name_exercise attribute or name_exercise is misspelled or invalid.")
                break

            exercise.difficulty_level   = row.get("difficulty_level") or ""
            exercise.type               = row.get("type") or ""

            target_muscle = row.get("target_muscle") 
            if (isinstance(target_muscle, list)):
                exercise.target_muscle  = ", ".join(target_muscle)
            elif ("target_muscle" in row) :
                exercise.target_muscle  = row.get("target_muscle") or ""
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

            exercise.difficulty_level   = row.get("difficulty_level") or ""
            exercise.instructions       = row.get("instructions") or ""
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

            if "name_food" in row and row["name_food"] != 0:
                food.name = row["name_food"]
            else :
                succesful = False
                WriteLog(file, "file does not contain name_food attribute or name_food is misspelled.")
                break

            food.category = row.get("category") or ""
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
    data["date"] = pandas.to_datetime(data["date"])

    users = session.query(User.user_id, User.email).all()

    user_map = {}

    for user in users:
        user_map[user.email] = user.user_id

    try: 
        for index,row in data.iterrows():
            healthMetric : Health_metric = Health_metric()

            if "user_email" in row and row["user_email"] != 0:
                healthMetric.user_id = user_map[row.get("user_email")] 

            if "date" in row and row["date"] != 0:
                healthMetric.date = row["date"]
            else :
                succesful = False
                WriteLog(file, "file does not contain date attribute or date is misspelled.")
                break

            if "start_weight" in row and row["start_weight"] != 0:
                healthMetric.start_weight = row["start_weight"]
            else:
                succesful = False
                WriteLog(file, "file does not contain start_weight attribute or start_weight is misspelled.")
                break

            if "current_weight" in row and row["current_weight"] != 0:
                healthMetric.current_weight = row["current_weight"]
            else:
                succesful = False
                WriteLog(file, "file does not contain current_weight attribute or current_weight is misspelled.")
                break

            if "avg_bpm" in row and row["avg_bpm"] != 0:
                healthMetric.avg_bpm = row["avg_bpm"]
            else:
                succesful = False
                WriteLog(file, "file does not contain avg_bpm attribute or avg_bpm is misspelled.")
                break

            if "max_bpm" in row and row["max_bpm"] != 0:
                healthMetric.max_bpm = row["max_bpm"]
            else:
                succesful = False
                WriteLog(file, "file does not contain max_bpm attribute or max_bpm is misspelled.")
                break

            if "resting_bpm" in row and row["resting_bpm"] != 0:
                healthMetric.resting_bpm = row["resting_bpm"]
            else:
                succesful = False
                WriteLog(file, "file does not contain resting_bpm attribute or resting_bpm is misspelled.")
                break

            if "steps_count" in row and row["steps_count"] != 0:
                healthMetric.steps_count = row["steps_count"]
            else:
                succesful = False
                WriteLog(file, "file does not contain steps_count attribute or steps_count is misspelled.")
                break

            if "sleep_time" in row and row["sleep_time"] != 0:
                healthMetric.sleep_time = row["sleep_time"]
            else:
                succesful = False
                WriteLog(file, "file does not contain sleep_time attribute or sleep_time is misspelled.")
                break

            if "calories_burned" in row and row["calories_burned"] != 0:
                healthMetric.calories_burned = row["calories_burned"]
            else:
                succesful = False
                WriteLog(file, "file does not contain calories_burned attribute or calories_burned is misspelled.")
                break

            if "active_minute" in row and row["active_minute"] != 0:
                healthMetric.active_minute = row["active_minute"]
            else:
                succesful = False
                WriteLog(file, "file does not contain active_minute attribute or active_minute is misspelled.")
                break

            if "workout_type" in row and row["workout_type"] != 0:
                healthMetric.workout_type = row["workout_type"]
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