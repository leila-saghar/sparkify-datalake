## sparkify-datalake
### Introduction
 <p>Sparkify Start-up has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
    In this project an ETL pipeline has been built to extract their data from S3, process them using Spark, and load the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.</p>

## Project Structure
**dl.cfg** this files contains your AWS credential, currently has empty value for your AWS Secret Key and value, please set the right value before running the project<br>
**etl.py** the etl pipeline that extracts songs and log data from s3, transform them using Spark and loads them in Fact and dimension tables in S3 in Parquet format<br>
**README.md** contains what you need to know about the project<br>


## Source Data:
**Song data**: s3://udacity-dend/song_data
**Log data**: s3://udacity-dend/log_data

##### example of Song data:
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Datalake Schema

#### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
#### Dimension Tables
**users** - users in the app<br>
user_id, first_name, last_name, gender, level<br>
**songs** - songs in music database<br>
song_id, title, artist_id, year, duration<br>
**artists** - artists in music database<br>
artist_id, name, location, lattitude, longitude<br>
**time** - timestamps of records in songplays broken down into specific units<br>
start_time, hour, day, week, month, year, weekday

## ETL Pipeline
1 - extract data from log_path and song_path and store them in a dataframe<br>
2 - transform data, take care of duplicate data, null values and apply requested partitioning <br>
3 - load data back into S3, each table in a separate directory ( same S3 bucket).<br>
** process time is use as a postfix for each table  <br>

### How to run
1 - Write correct keys in dl.cfg<br>
2 - a. Open Terminal write the command "python etl.py"<br>

    b. Generate jar file using command below.
    b.i java -jar etl.jar 
    b.ii upload etl.jar file in your S3
    b.iii submit a 'step' to your EMR Cluster, using the jar file you created in the previous step

    c. ssh to your emr cluster, use the spark-submit command to submit the jar file you created into spark
