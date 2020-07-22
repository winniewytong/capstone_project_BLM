import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
import logging
import s3fs

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def check_ny_crimes(spark,output_data):
    ny_original_count = 5012956
    ny_data = os.path.join(output_data,'ny_crimes/*.parquet')
    ny = spark.read.parquet(ny_data)
    ny_records = ny.count()
    if ny_records != ny_original_count:
        raise ValueError(f"Data quality check failed. ny_records returned wrong results")

def check_date(spark,output_data):
    date_data = os.path.join(output_data,'date/*.csv')
    dateDf = spark.read.parquet(date_data)
    date_count = dateDf.count()
    if date_count < 1:
        raise ValueError(f"Data quality check failed. dateDf returned no results")
        
def check_general_offense_category(spark,output_data):
    general_offense_category = os.path.join(output_data,'general_offense_category/*.parquet')
    general_offense_categoryDF = spark.read.parquet(general_offense_category)
    general_offense_category_record = general_offense_categoryDF.count()
    if general_offense_category_record < 1:
        raise ValueError(f"Data quality check failed. general_offense_category returned no results")

def check_police_dept_offense_category(spark,output_data):
    police_dept_offense_category = os.path.join(output_data,'police_dept_offense_category/*.parquet')
    police_dept_offense_categoryDF = spark.read.parquet(police_dept_offense_category)
    police_dept_offense_category_record = police_dept_offense_categoryDF.count()
    if police_dept_offense_category_record < 1:
        raise ValueError(f"Data quality check failed. police_dept_offense_category returned no results")

def check_race(spark,output_data):
    race = os.path.join(output_data,'race/*.parquet')
    raceDf = spark.read.parquet(race)
    race_record = raceDf.count()
    if race_record < 1:
        raise ValueError(f"Data quality check failed. race returned no results")

def check_victims(spark,output_data):
    victims = os.path.join(output_data,'victims/*.csv')
    victimsDf = spark.read.parquet(victims)
    victims_records = victimsDf.count()
    if victims_records < 1:
        raise ValueError(f"Data quality check failed. victims returned no results")

def check_police_death(spark,output_data):
    police_death = os.path.join(output_data,'police_death/*.csv')
    police_deathDf = spark.read.parquet(police_death)
    police_death_records = police_deathDf.count()
    if police_death_records < 1:
        raise ValueError(f"Data quality check failed. police_death returned no results")

def check_city_population(spark,output_data):
    city_population = os.path.join(output_data,'city_population/*.csv')
    city_populationDf = spark.read.parquet(city_population)
    city_population_records = city_populationDf.count()
    if city_population_records < 1:
        raise ValueError(f"Data quality check failed. city_population returned no results")

def check_state(spark,output_data):
    state = os.path.join(output_data,'state/*.csv')
    stateDf = spark.read.parquet(state)
    state_record = stateDf.count()
    if state_record < 1:
        raise ValueError(f"Data quality check failed. state returned no results")
        
def check_protest(spark,output_data):
    protest_data = os.path.join(output_data,'protest/*.csv')
    protest = spark.read.parquet(protest_data)
    protest_recrods = protest.count()
    if protest_recrods < 1:
        raise ValueError(f"Data quality check failed. protest returned no results")

        
        
def main():
    spark = create_spark_session()

    output_data = "s3a://datalake-blm/processed/"
    
    check_ny_crimes(spark, output_data)
    check_general_offense_category(spark, output_data) 
    check_police_dept_offense_category(spark, output_data) 
    check_race(spark, output_data) 
    check_victims(spark, output_data) 
    check_police_death(spark, output_data) 
    check_city_population(spark, output_data) 
    check_state(spark, output_data) 
    check_protest(spark, output_data)
    check_date(spark, output_data)


if __name__ == "__main__":
    main()