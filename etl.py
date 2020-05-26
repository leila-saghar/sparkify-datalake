from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import Window
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc, last
import configparser
from datetime import datetime
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark import SparkContext
from pyspark import SparkConf
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID") # config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")# config['AWS_SECRET_ACCESS_KEY']

logs_path = "s3a://udacity-dend/log_data/*/*/*.json"     
song_path = "s3a://udacity-dend/song_data/A/B/C/*.json"
run_start_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')

spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
		
		
		
		

# get filepath to song data file   
songs_data = spark.read.json(song_path)
# read song data file 
###################################       
# extract columns to create songs table 
songs_table = songs_data.select(["song_id", "title", "artist_id", "year", "duration"]).dropDuplicates()

# write songs table to parquet files partitioned by year and artist
run_start_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
start_st = datetime.now()
songs_table_path = "s3a://udacity-dend-test/songs_" + run_start_time

# Write DF to Spark parquet file (partitioned by year and artist_id)
print("Writing songs_table parquet files to {}...".format(songs_table_path))

songs_table.write.mode("overwrite").partitionBy("year", "artist_id")\
        .parquet(songs_table_path)
stop_st = datetime.now()
#total_st = stop_st - start_st
print("...finished writing songs_table in {}.".format(stop_st - start_st))
###################################
# extract columns to create artists table
artist_table = songs_data.select(["artist_Id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])\
                            .dropDuplicates()\
                            .withColumnRenamed("artist_name","name")\
                            .withColumnRenamed("artist_location","location")\
                            .withColumnRenamed("artist_latitude","lattitude")\
                            .withColumnRenamed("artist_longitude","longitude")

artist_table_path = "s3a://udacity-dend-test/artists_" + run_start_time
# Write DF to Spark parquet file (partitioned by year and artist_id)
# write artists table to parquet files
print("Writing artist_table parquet files to {}...".format(artist_table_path))
artist_table.write.mode("overwrite").parquet(artist_table_path)    


# get filepath to log data file    
log_data = spark.read.json(logs_path)
###########################################################
# read log data file
user_table = log_data\
.filter(log_data.page == 'NextSong' )\
.filter(log_data.userId != '' )\
.select(["userId", "firstName", "lastName", "gender", "level", "ts"])\
.orderBy("userId")

partitionWindow = Window\
.partitionBy("userId")\
.orderBy("ts")\
.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

user_table_valid = user_table\
.withColumn("newlevel", (last("level").over(partitionWindow)))

user_table = user_table_valid\
.select(["userId", "firstName", "lastName", "gender", "newlevel"])\
.dropDuplicates()\
.withColumnRenamed("newlevel","level") 

user_table_path = "s3a://udacity-dend-test/users_" + run_start_time

# Write DF to Spark parquet file (partitioned by year and artist_id)
print("Writing songs_table parquet files to {}...".format(user_table_path))
user_table_valid.write.mode("overwrite").parquet(user_table_path) 
###########################################################   
# create timestamp column from original timestamp column
#start_time, hour, day, week, month, year, weekday, partitioned by year and month

get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0).hour)
get_day = udf(lambda x: datetime.fromtimestamp(x / 1000.0).day)
get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)
get_weekday = udf(lambda x: datetime.fromtimestamp(x / 1000.0).weekday())
# extract columns to create time table
time_data = log_data.select("ts").dropDuplicates()   

time_table = time_data\
.withColumn("hour", get_hour(time_data.ts).cast('int'))\
.withColumn("day", get_day(time_data.ts).cast('int'))\
.withColumn("month", get_month(time_data.ts).cast('int'))\
.withColumn("year", get_year(time_data.ts).cast('int'))\
.withColumn("weekday", get_weekday(time_data.ts).cast('int'))\
.withColumnRenamed("ts", "start_time")   

# write time table to parquet files partitioned by year and month    
start_st = datetime.now()
time_table_path = "s3a://udacity-dend-test/times_" + run_start_time

# Write DF to Spark parquet file (partitioned by year and artist_id)
print("Writing time parquet files to {}...".format(time_table_path))

time_table.write.mode("overwrite")\
.partitionBy("year", "month")\
.parquet(time_table_path)

stop_st = datetime.now()
total_st = stop_st - start_st
print("...finished writing songs_table in {}.".format(total_st))
###########################################################
# read in song data to use for songplays table
get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)

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
        songs_data.artist_id,songs_data.song_id).withColumn("partition_month", get_month(log_data.ts).cast('int')).withColumn("partition_year", get_year(log_data.ts).cast('int'))

# extract columns from joined song and log datasets to create songplays table 
start_st = datetime.now()
songplays_table_path = "s3a://udacity-dend-test/songplays_" + run_start_time

# Write DF to Spark parquet file (partitioned by year and artist_id)
print("Writing songplays_data parquet files to {}...".format(songplays_table_path))

songplays_data.write.mode("overwrite")\
.partitionBy("partition_year", "partition_month")\
.parquet(songplays_table_path)

stop_st = datetime.now()
total_st = stop_st - start_st
print("...finished writing songplays_data in {}.".format(total_st)) 

# write songplays table to parquet files partitioned by year and month


