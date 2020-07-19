############################################################################################################################
#  Udacity Data Engineering Nanodegree - ASSIGNMENT-4
#  
#  Apply the knowledge of Spark and Data Lakes to build and ETL pipeline for a Data Lake hosted on Amazon S3
#  The ETL Pipeline below extracts the data from S3 and process them using Spark 
#  then load data in Parquet format back into S3 in a set of Fact and Dimension Tables. 
#  Aparkify's analytics team use the data in the datalake for further insights in what songs their users are listening. 
#  To get full benefit of Sparkm this pipeline will be submitted on AWS EMR Cluster.
############################################################################################################################
import configparser
from datetime import datetime
import os
import pyspark
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, last
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark import SparkContext
from pyspark import SparkConf

################ Get Credential from config file #################
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID") 
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")
destination_path = 's3a://sparkify-datalake-2020/'


# create Spark Session
def create_spark_session():        
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, songs_data):
    
    ############################### SONGS TABLE ############################
    
    # extract columns to create songs table and drop duplicate records
    songs_table = songs_data.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()

    # write songs_table dataframe to parquet files partitioned by year and artistId    
    songs_table_path = destination_path + "songs"
    print("Writing songs_table parquet files to {}...".format(songs_table_path))
    start_st = datetime.now() 
    songs_table.write.mode("overwrite")\
    .partitionBy("year", "artist_id")\
    .parquet(songs_table_path)
    
    stop_st = datetime.now()
    print("...finished writing songs_table in {}.".format(stop_st - start_st))
    
    ############################### ARTISTS TABLE ############################
    # extract columns to create artists table
    artist_table = songs_data.select(["artist_Id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])\
        .dropDuplicates()\
        .withColumnRenamed("artist_name","name")\
        .withColumnRenamed("artist_location","location")\
        .withColumnRenamed("artist_latitude","lattitude")\
        .withColumnRenamed("artist_longitude","longitude")
    
    # write artists table to parquet files
    artist_table_path = destination_path+ "artists" 
    print("Writing artist_table parquet files to {}...".format(artist_table_path))
    
    artist_start_st = datetime.now()
    artist_table.write.mode("overwrite")\
        .parquet(artist_table_path)    
    artist_stop_st = datetime.now()
    print("...finished writing artist_table in {}.".format(artist_stop_st - artist_start_st))

def process_log_data(spark, logs_path, songs_data):
        
        # get filepath to log data file    
        log_data = spark.read.json(logs_path)
        ############################### USER TABLE ############################
        """
        filter log data where page is 'NextPage' then select the required columns for user table. 
        exclude any record that doesn't have valid userId
        here window function is used as we are intersted to have the latest status of each user,
        in case user moved from paid to free or vice versa.
        """ 
        #read log data file
        user_table = log_data\
        .filter(log_data.page == 'NextSong' )\
        .filter(log_data.userId != '' )\
        .select(["userId", "firstName", "lastName", "gender", "level", "ts"])\
        .orderBy("userId")

        
        partitionWindow = Window\
        .partitionBy("userId")\
        .orderBy("ts")\
        .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        # get latest version of each user
        user_table_valid = user_table\
        .withColumn("newlevel", (last("level")\
                                .over(partitionWindow)))

        # extract the required properties for user table 
        user_table = user_table_valid\
        .select(["userId", "firstName", "lastName", "gender", "newlevel"])\
        .dropDuplicates()\
        .withColumnRenamed("newlevel","level") 

        # start writing user data in parquet format in S3        
        user_table_path = destination_path + "users" 

        # Write DF to Spark parquet file (partitioned by year and artist_id)
        print("Writing user_table parquet files to {}...".format(user_table_path))
        start_st = datetime.now()

        user_table_valid.write.mode("overwrite")\
        .parquet(user_table_path) 
        stop_st = datetime.now()
        print("...finished writing user_table in {}.".format(stop_st - start_st))

        ############################### TIME TABLE ############################   
        """
        create timestamp column from original timestamp column
        used udf to extract year, month,...
        """

        get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0).hour)
        get_day = udf(lambda x: datetime.fromtimestamp(x / 1000.0).day)
        get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
        get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)
        get_weekday = udf(lambda x: datetime.fromtimestamp(x / 1000.0).weekday())
        
        # extract ys column to create time table and drop all duplicate values
        time_data = log_data.select("ts").dropDuplicates()   

        # create time_table dataframe
        time_table = time_data\
        .withColumn("hour", get_hour(time_data.ts).cast('int'))\
        .withColumn("day", get_day(time_data.ts).cast('int'))\
        .withColumn("month", get_month(time_data.ts).cast('int'))\
        .withColumn("year", get_year(time_data.ts).cast('int'))\
        .withColumn("weekday", get_weekday(time_data.ts).cast('int'))\
        .withColumnRenamed("ts", "start_time")   

        
        time_table_path = destination_path + "time"         
        print("Writing time parquet files to {}...".format(time_table_path))

        # write time_table to parquet files in S3 partitioned by year and month    
        time_start_st = datetime.now()
        time_table.write.mode("overwrite")\
        .partitionBy("year", "month")\
        .parquet(time_table_path)
        time_stop_st = datetime.now()  
        print("...finished writing time_table in {}.".format(time_stop_st - time_start_st ))

        ############################### SONGPLAY TABLE ############################   
        """
        create songPlay table using songs and user_logs data, songPlay is our fact table
        
        """
        # extract month and year as they are needed for partitioning later
        get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
        get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)

        # read in song data to use for songplays table
        # extract columns from joined song and log datasets to create songplays table
        songplays_data = log_data\
        .join(songs_data,\
        ((log_data.artist == songs_data.artist_name) & \
        (log_data.song == songs_data.title)),\
        how='left')\
        .select(log_data.ts,\
                log_data.userId,\
                log_data.level,\
                log_data.sessionId,\
                log_data.location,\
                log_data.userAgent,\
                songs_data.artist_id,songs_data.song_id)\
                .withColumn("partition_month", get_month(log_data.ts).cast('int'))\
                .withColumn("partition_year", get_year(log_data.ts).cast('int'))

         
        
        songplays_table_path = destination_path + "songplays"         
        print("Writing songplays_data parquet files to {}...".format(songplays_table_path))

        # write songplays table to parquet files partitioned by year and month
        songplays_start_st = datetime.now()
        songplays_data.write.mode("overwrite")\
        .partitionBy("partition_year", "partition_month")\
        .parquet(songplays_table_path)
        songplays_stop_st = datetime.now()       
        print("...finished writing songplays_data in {}.".format(songplays_stop_st - songplays_start_st))        

"""
    entry point of the etl pipeline
    1 - extract data from log_path and song_path and store them in a dataframe
    2 - transform data, take care of duplicate data, null values and apply requested partitioning 
    3 - load data back into S3, each table in a separate directory ( same S3 bucket).
    ** process time is use as a postfix for each table  
"""
def main():
    spark = create_spark_session()
 
    logs_path = "s3a://udacity-dend/log_data/*/*/*.json"     
    song_path = "s3a://udacity-dend/song_data/A/B/C/*.json"
    #song_path = 's3a://udacity-dend/song_data/*/*/*/*.json'
    # get filepath to log data file    
    # read song data file s3 and store in dataframe
    print("read song data")
    songs_data = spark.read.json(song_path)
    print("process_song_data")
    process_song_data(spark, songs_data)    
    print("process_log_data")
    process_log_data(spark, logs_path, songs_data)


if __name__ == "__main__":
    main()
    
