import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month,dayofweek, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
import logging



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


def process_ny_crime(spark, input_data, output_data):
    """
    This function reads ny crimes data from S3, loads the massive amount of data using spark,
    and placing the transformed data on S3. 
    
    Parementers:
        spark: using spark session 
        input_data: source data from udacity S3 bucket
        out_data: destination of data in S3 bucket after transformation
    """
    
    ny_data = os.path.join(input_data,'ny_crimes/*.parquet')
       
    ny = spark.read.parquet(ny_data)


    ny_table = ny.select('arrest_key','arrest_date','pd_code','offense_code','law_code',\
                           'law_cat_code','arrest_borough','arrest_precinct','jurisdiction_code',\
                           'age_group','perp_gender','perp_race','x_coord_code','y_coord_code',\
                           'latitude','longitude','lon_lat').withColumn('year',year(col('arrest_date')))
    
    ny_table.write.parquet(os.path.join(output_data,'ny_crimes/'),'overwrite')
    logging.info('ny_table saved')
    
    date=ny.select(col('arrest_date').alias('calendar_date')).orderBy('calendar_date', ascending=True)

    dateDf = date.withColumn('year',year(col('calendar_date')))\
        .withColumn('month',month(col('calendar_date')))\
        .withColumn('day',dayofmonth(col('calendar_date')))\
        .withColumn('week',weekofyear(col('calendar_date')))\
        .withColumn('weekday',dayofweek(col('calendar_date')))\
    .dropDuplicates()
    
    dateDf.write.parquet(os.path.join(output_data,'date/'),'overwrite')
    logging.info('date_table saved')
    
    general_offense = ny.select('offense_code','offense_desc').dropDuplicates()
    
    general_offense.write.parquet(os.path.join(output_data,'general_offense_category/'),'overwrite')
    logging.info('general_offense_category saved')
    
    police_dept_offense = ny.select('pd_code','pd_desc').dropDuplicates()
    
    police_dept_offense.write.parquet(os.path.join(output_data,'police_dept_offense_category/'),'overwrite')
    logging.info('police_dept_offense_category saved')
    
    
    race = ny.select(col('perp_race').alias('race')).dropDuplicates().withColumn('race_id', F.monotonically_increasing_id())
    
    race.write.parquet(os.path.join(output_data,'race/'),'overwrite')
    logging.info('race saved')
    

def process_other_dfs(spark, input_data, output_data):
    """
    This function reads other datasets from S3, loads the massive amount of data using spark,
    and placing the transformed data on S3. The mait purpose of this function is to move datsets from 
    stage bucket to processed bucket. More transformation jobs can be done in the future. 
    
    Parementers:
        spark: using spark session 
        input_data: source data from udacity S3 bucket
        out_data: destination of data in S3 bucket after transformation
    """  
    
    state = os.path.join(input_data,'state.csv')    
    stateDf = spark.read.csv(state)
    stateDf.write.csv(os.path.join(output_data, 'state'),mode='overwrite')
    logging.info('state saved')   
    
    protest = os.path.join(input_data,'protest.csv') 
    protestDF = spark.read.csv(protest)    
    protestDF.write.csv(os.path.join(output_data, 'protest'),mode='overwrite')
    logging.info('protest saved')
    
    victims = os.path.join(input_data,'victims.csv') 
    victimsDf = spark.read.csv(victims)    
    victimsDf.write.csv(os.path.join(output_data, 'victims'),mode='overwrite')
    logging.info('victims saved')   

    police_death = os.path.join(input_data,'police_death.csv') 
    police_deathDf = spark.read.csv(police_death)    
    police_deathDf = police_deathDf.withColumn('police_death_id', F.monotonically_increasing_id())
    police_deathDf.write.csv(os.path.join(output_data, 'police_death'),mode='overwrite')
    logging.info('police_death saved')
    
    city_population = os.path.join(input_data,'city_population.csv') 
    city_populationDf = spark.read.csv(city_population)    
    city_populationDf.write.csv(os.path.join(output_data, 'city_population'),mode='overwrite')
    logging.info('city_population saved')   


def main():
    spark = create_spark_session()

    input_data = "s3a://datalake-blm/stage/"
    output_data = "s3a://datalake-blm/processed/"
    
    #process_ny_crime(spark, input_data, output_data)    
    process_other_dfs(spark, input_data, output_data)


if __name__ == "__main__":
    main()