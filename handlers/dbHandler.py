import pandas

from typing import Any, List
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import Column
from config import Consume, Exercise, Food, Health_metric, Practice, User
from utils.fileManager import MoveToArchive, MoveToError, WriteLog
from utils.dataframeFormatter import formatDataFrame

def sendToTable(data: pandas.DataFrame, file: str, session: Session):
    numericDispatch = {
        "1": sendUserToDb,
        "2": sendExerciseToDb,
        "3": sendFoodToDb,
        "4": sendHealthMetricToDb,
        "5": sendUserFoodRelationToDb,
        "6": sendUserExerciseRelationToDb,
    }

    nameDispatch = {
        "user": sendUserToDb,
        "exercise": sendExerciseToDb,
        "food": sendFoodToDb,
        "health": sendHealthMetricToDb,
        "consume": sendUserFoodRelationToDb,
        "practice": sendUserExerciseRelationToDb,
    }

    fileTableNumber: str = file[0]

    if fileTableNumber.isnumeric() :
        if fileTableNumber in numericDispatch:
            return numericDispatch[fileTableNumber](data, file, session)
        else :
            WriteLog(file, "no matches with a table, index out of range 6")
            MoveToError(file)
            return
        
    fileLowered = file.lower()
    
    for name, func in nameDispatch.items():
        if name in fileLowered:
            return func(data, file, session)
    
    WriteLog(file, "no matches with a table, no index or name found")
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

    try: 
        if "birthdate" in data:
            data["birthdate"] = pandas.to_datetime(data["birthdate"]).dt.date
    
        if "subscription_date" in data:
            data["subscription_date"] = pandas.to_datetime(data["subscription_date"]).dt.date

        errorMessage = ""

        for _,row in data.iterrows():

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

            gender = row.get("gender")
            if "gender" in row and isinstance(gender, str) and gender.lower() in ['male', 'female', 'other']:
                user.gender = gender.lower()
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
            if "physical_activity_level" in row and isinstance(physicalActivityLevel, str) and physicalActivityLevel.lower() in ['sedentary', 'moderate', 'active']:
                user.physical_activity_level = physicalActivityLevel.lower()
            else :
                errorMessage += "file does not contain physical_activity_level attribute or physical_activity_level is misspelled or invalid.\n"

            user.daily_caloric_intake ,placeHolderMessage = addData(row, "daily_caloric_intake")
            errorMessage += placeHolderMessage

            user.goal ,placeHolderMessage = addData(row, "goal")
            if placeHolderMessage != "":
                user.goal = "Non renseigné"

            subscription = row.get("subscription")
            if "subscription" in row and isinstance(subscription, str) and subscription.lower() in ['freemium', 'premium', 'premium+']:
                user.subscription = subscription.lower()
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
        for _,row in data.iterrows():
            exercise: Exercise = Exercise()

            exercise.name ,placeHolderMessage = addData(row, "name")
            errorMessage += placeHolderMessage

            exercise.difficulty_level ,placeHolderMessage = addData(row, "difficulty_level")
            if exercise.difficulty_level is None:
                exercise.difficulty_level = "Non renseigné"

            exercise.type ,placeHolderMessage = addData(row, "type")
            if exercise.type is None:
                exercise.type = "Non renseigné"

            target_muscle = row.get("target_muscle") 
            if (isinstance(target_muscle, list)):
                exercise.target_muscle  = ", ".join(target_muscle)
            elif ("target_muscle" in row) :
                exercise.target_muscle  = row.get("target_muscle") or "Non renseigné"
            else :
                errorMessage += "file does not contain target_muscle attribute or target_muscle is misspelled or invalid.\n"

            secondaryMuscle = row.get("secondary_muscle") or "No Secondary Muscle"
            if (isinstance(secondaryMuscle, list)) :
                exercise.secondary_muscle  = ", ".join(row.get("secondary_muscle") or [])
            elif ("secondary_muscle" in row) :
                exercise.secondary_muscle  = secondaryMuscle
            else :
                exercise.secondary_muscle = "Non renseigné"

            equipment = row.get("equipment") or "Non renseigné"
            if (isinstance(equipment, list)) :
                exercise.equipment  = ", ".join(row.get("equipment") or [])
            elif ("equipment" in row) :
                exercise.equipment = equipment
            else :
                exercise.equipment = "Non renseigné"

            exercise.instructions ,placeHolderMessage = addData(row, "instructions")
            if exercise.instructions is None:
                exercise.instructions = "Non renseigné"

            constraints = row.get("constraints") 
            if (isinstance(constraints, list)):
                exercise.constraints = ", ".join(constraints)
            elif ("constraints" in row) :
                exercise.constraints = row.get("constraints") or "Non renseigné"
            else :
                exercise.constraints = "Non renseigné"
                
            session.add(exercise)
        
        if errorMessage != "":
            succesful = False
            WriteLog(file, errorMessage)

        if (succesful):
            session.commit()      
    except Exception as ex:
        succesful = False
        session.rollback()
        WriteLog(file, str(ex))
    if (succesful):
        MoveToArchive(file)
    else :
        MoveToError(file)

def sendFoodToDb(data: pandas.DataFrame, file: str, session: Session) :
    succesful : bool = True

    errorMessage = ""
    field = [
        "name",
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

    errorMessage = ""

    try: 
        for _,row in data.iterrows():

            food: Food = Food()

            if "name" in row and row.get("name") != 0:
                food.name = row.get("name")
            else :
                errorMessage += "file does not contain name attribute or name is misspelled or invalid.\n"

            food.category = row.get("category", "Non renseigné") or "Non renseigné"
            food.calories = row.get("calories", 0) 
            food.protein = row.get("protein", 0) 
            food.carbohydrates = row.get("carbohydrates", 0) 
            food.fat = row.get("fat", 0)
            food.fiber = row.get("fiber", 0)
            food.sugars = row.get("sugars", 0)

            sodium = row.get("sodium", 0)  
            if sodium > 32767:
                WriteLog(file, food.name + "sodium is above smallint limit")
                succesful = False
                break
            else : 
                food.sodium = sodium

            cholestorol = row.get("cholestorol", 0) 
            if cholestorol > 32767:
                WriteLog(file, food.name + " cholesterol is above smallint limit")
                succesful = False
                break
            else :
                food.cholestorol = cholestorol

            session.add(food)
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

    if "date" in data:
        data["date"] = pandas.to_datetime(data["date"])

    users = session.query(User.user_id, User.email).all()
    user_map = {}
    for user in users:
        user_map[user.email] = user.user_id

    errorMessage = ""
    try: 
        for _,row in data.iterrows():
            healthMetric : Health_metric = Health_metric()

            email = row.get("user_email")
            if "user_email" in row and email != 0:
                if email in user_map:
                    healthMetric.user_id = user_map[email]
                else :
                    errorMessage += "no user with that email"                    
            else :
                errorMessage += "file does not contain user_email attribute or user_email is misspelled."              

            healthMetric.date_ ,placeHolderMessage = addData(row, "date")
            errorMessage += placeHolderMessage

            healthMetric.start_weight ,placeHolderMessage = addData(row, "start_weight")
            errorMessage += placeHolderMessage

            healthMetric.current_weight ,placeHolderMessage = addData(row, "current_weight")
            errorMessage += placeHolderMessage

            healthMetric.avg_bpm ,placeHolderMessage = addData(row, "avg_bpm")
            errorMessage += placeHolderMessage

            healthMetric.max_bpm ,placeHolderMessage = addData(row, "max_bpm")
            errorMessage += placeHolderMessage

            healthMetric.resting_bpm ,placeHolderMessage = addData(row, "resting_bpm")
            errorMessage += placeHolderMessage

            healthMetric.steps_count ,placeHolderMessage = addData(row, "steps_count")
            errorMessage += placeHolderMessage

            healthMetric.sleep_time ,placeHolderMessage = addData(row, "sleep_time")
            errorMessage += placeHolderMessage

            healthMetric.calories_burned ,placeHolderMessage = addData(row, "calories_burned")
            errorMessage += placeHolderMessage

            healthMetric.active_minute ,placeHolderMessage = addData(row, "active_minute")
            errorMessage += placeHolderMessage

            healthMetric.workout_type ,placeHolderMessage = addData(row, "workout_type")
            errorMessage += placeHolderMessage

            session.add(healthMetric)
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

def sendUserFoodRelationToDb(data: pandas.DataFrame, file: str, session: Session):
    succesful : bool = True

    errorMessage = ""
    field = [
        "email",
        "food_name"
    ]

    data, errorMessage = formatDataFrame(data, field)
    if errorMessage != "":
        WriteLog(file, errorMessage)
        MoveToError(file)
        return

    try: 
        emails: list[Any] = data["email"].tolist()
        foodNames: list[Any] = data["food_name"].tolist()

        users: List[User] = session.query(User).filter(User.email.in_(emails)).all()
        foods: List[Food] = session.query(Food).filter(Food.name.in_(foodNames)).all()

        userMap: dict[Column[str], User] = {u.email: u for u in users}
        foodMap: dict[Column[str], Food] = {f.name: f for f in foods}

        for _,row in data.iterrows():
            consume : Consume = Consume()

            consume.food_id = foodMap.get(row.get("food_name")).food_id
            consume.user_id = userMap.get(row.get("email")).user_id

            session.add(consume)
        
        if succesful :
            session.commit()

    except Exception as ex:
        succesful = False
        session.rollback()
        WriteLog(file, str(ex))
    if (succesful):
        MoveToArchive(file)
    else :
        MoveToError(file)

def sendUserExerciseRelationToDb(data: pandas.DataFrame, file: str, session: Session):
    succesful : bool = True

    errorMessage = ""
    field = [
        "email",
        "exercise_name"
    ]

    data, errorMessage = formatDataFrame(data, field)
    if errorMessage != "":
        WriteLog(file, errorMessage)
        MoveToError(file)
        return

    try: 
        emails: list[Any] = data["email"].tolist()
        exerciseNames: list[Any] = data["exercise_name"].tolist()

        users: List[User] = session.query(User).filter(User.email.in_(emails)).all()
        exercises: List[Exercise] = session.query(Exercise).filter(Exercise.name.in_(exerciseNames)).all()

        userMap: dict[Column[str], User] = {u.email: u for u in users}
        exerciseMap: dict[Column[str], Exercise] = {e.name: e for e in exercises}

        for _,row in data.iterrows():
            practice : Practice = Practice()

            practice.exercise = exerciseMap.get(row.get("exercise_name"))
            practice.user = userMap.get(row.get("email"))

            session.add(practice)
        
        if succesful :
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
