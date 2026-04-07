import os
from sqlalchemy import NUMERIC, SMALLINT, TEXT, TIMESTAMP, Column, Date, DateTime, ForeignKey, Numeric, SmallInteger, String, Integer, Text, Time, Table
from sqlalchemy.orm import DeclarativeBase, relationship

BASE_PATH      = os.path.dirname(os.path.abspath(__file__))
ARCHIVE_PATH   = os.path.join(BASE_PATH, "Archive")
ERROR_PATH     = os.path.join(BASE_PATH, "Error")
LOG_PATH       = os.path.join(BASE_PATH, "Log")
TO_IMPORT_PATH = os.path.join(BASE_PATH, "ToImport")

class Base(DeclarativeBase):
    pass

class Consume(Base):
    __tablename__ = "consume"

    consume_id = Column(Integer, primary_key=True)
    user_id = Column(ForeignKey("user_.user_id"))
    food_id = Column(ForeignKey("food.food_id"))

    user = relationship("User", back_populates="consumes")
    food = relationship("Food", back_populates="consumes")

class Practice(Base):
    __tablename__ = "practice"

    practice_id = Column(Integer, primary_key=True)
    user_id = Column(ForeignKey("user_.user_id"))
    exercise_id = Column(ForeignKey("exercise.exercise_id"))

    user = relationship("User", back_populates="practice")
    exercise = relationship("Exercise", back_populates="practice")

class User(Base) :
    __tablename__           = "user_"

    user_id                 = Column(Integer, primary_key=True, autoincrement=True)
    first_name              = Column(String(50))
    last_name               = Column(String(50))
    email                   = Column(String(100), unique=True)
    password                = Column(String(50))
    birthdate               = Column(Date)
    gender                  = Column(String(50))
    weight                  = Column(NUMERIC(15,2))
    height                  = Column(Integer)
    bmi                     = Column(NUMERIC(15,2))
    body_fat_pct            = Column(NUMERIC(15,2))
    physical_activity_level = Column(String(50))
    daily_caloric_intake    = Column(Integer)
    goal                    = Column(TEXT)
    subscription            = Column(String(50))
    date_subscription       = Column(Date)
    constraints            = Column(Text)

    consumes = relationship("Consume", back_populates="user")
    practice = relationship("Practice", back_populates="user")

class Food(Base) :
    __tablename__   = "food"

    food_id         = Column(Integer, primary_key=True, autoincrement=True)
    name            = Column(String(100))
    category        = Column(String(50))
    calories        = Column(NUMERIC(15,2))
    protein         = Column(NUMERIC(15,2))
    carbohydrates   = Column(NUMERIC(15,2))
    fat             = Column(NUMERIC(15,2))
    fiber           = Column(NUMERIC(15,2))
    sugars          = Column(NUMERIC(15,2))
    sodium          = Column(SMALLINT)
    cholestorol     = Column(SMALLINT)

    consumes = relationship("Consume", back_populates="food")

class Exercise(Base) :

    __tablename__       = "exercise"
    
    exercise_id         = Column(Integer, primary_key=True, autoincrement=True)
    name                = Column(String(50), nullable=False)
    type                = Column(String(50), nullable=False)
    target_muscle       = Column(Text, nullable=False)
    secondary_muscle    = Column(Text, nullable=False)
    equipment           = Column(Text, nullable=False)
    difficulty_level    = Column(String(50), nullable=False)
    instructions        = Column(Text, nullable=False)
    constraints         = Column(Text)

    practice = relationship("Practice", back_populates="exercise")

class Health_metric(Base) :
    __tablename__ = "health_metric"

    health_metric_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id          = Column(Integer, ForeignKey("user_.user_id"), nullable=False)
    date_            = Column(DateTime, nullable=False)
    start_weight     = Column(Numeric(15, 2), nullable=False)
    current_weight   = Column(Numeric(15, 2), nullable=False)
    avg_bpm          = Column(Numeric(15, 2), nullable=False)
    max_bpm          = Column(Numeric(15, 2), nullable=False)
    resting_bpm      = Column(Numeric(15, 2), nullable=False)
    steps_count      = Column(SmallInteger, nullable=False)
    sleep_time       = Column(Time, nullable=False)
    calories_burned  = Column(Numeric(15, 2), nullable=False)
    active_minute    = Column(Numeric(15, 2), nullable=False)
    workout_type     = Column(String(50), nullable=False)