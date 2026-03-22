import pandas

from sqlalchemy.orm import Session
from config import Exercise, Food, User
from fileManager import MoveToArchive, MoveToError, WriteLog

def sendToTable(data: pandas.DataFrame, file: str, session: Session):
    attributeNameList: pandas.Index[str] = data.columns
    
    nameName = ""
    for attributeName in attributeNameList:
        if(attributeName.startswith("name")):
            nameName = attributeName
            break
    
    match nameName:
        case "name_user":
            sendUserToDb(data, file ,session)
        case "name_exercise":
            sendExerciseToDb(data, file, session)
        case "name_food":
            sendFoodToDb(data, file, session)
        case _:
            WriteLog(file, "no matches with a table")
            MoveToError(file)

def sendUserToDb(data: pandas.DataFrame, file: str, session: Session):
    succesful : bool = True
    data = data.fillna(0)
    data["birthdate"] = pandas.to_datetime(data["birthdate"])
    data["subscription_date"] = pandas.to_datetime(data["subscription_date"])
    try: 
        for index,row in data.iterrows():

            user : User = User()

            if "name_user" in row and row["name_user"] != 0:
                user.name = row["name_user"]
            else :
                succesful = False
                WriteLog(file, "file does not contain name_user attribute or name_user is misspelled or invalid.")
                break

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
                user.last_name = row["first_name"]
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

            if "bithdate" in row and row["bithdate"] != 0:
                user.birthdate = row["bithdate"]
            else :
                succesful = False
                WriteLog(file, "file does not contain bithdate attribute or bithdate is misspelled or invalid.")
                break

            gender = row["gender"]
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

