# BUILD Data Lake and ETL process to analyze Sparkify Data

#### Author
Rohan Pradhan

### Summary
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. This project deals with building ETL processes to load the data provided in a S3 bucket into set of parquet files after transforming the data using Spark.

### Sources of Data
1. Songs dataset: 's3://udacity-dend/song_data'
2. Log Dataset: 's3://udacity-dend/log_data'

### Schema details
A star schema has been designed which consists of the following Fact and Dimension tables. 

#### Fact table:
1. songplays - records in log data associated with song plays
    - songplay_id
    - start_time
    - user_id
    - level
    - song_id
    - artist_id
    - session_id
    - location
    - user_agent

#### Dimension tables:
1. users - users in the app
    - user_id
    - first_name
    - last_name
    - gender
    - level
2. songs - songs in music database
    - song_id
    - title
    - artist_id
    - year
    - duration
3. artists - artists in music database
    - artist_id
    - name
    - location
    - latitude
    - longitude
4. time - timestamps of records in songplays broken down into specific units
    - start_time
    - hour
    - day
    - week
    - month
    - year
    - weekday

### Output Parquet Files
The parquet files written into separate directories for each table in the 'sparkify-warehouse' directory in the AWS S3 bucket specified in the config file as AWS_SPARKIFY_OUTPUT_DATA:
1. songs_table - partitionBy("year", "artist_id"):
    - [AWS_SPARKIFY_OUTPUT_DATA]/spark-warehouse/songs_table/
2. artists_table:
    - [AWS_SPARKIFY_OUTPUT_DATA]/spark-warehouse/artists_table/
3. users_table: 
    - [AWS_SPARKIFY_OUTPUT_DATA]/spark-warehouse/users_table/
4. time_table - partitionBy("year","month"):
    - [AWS_SPARKIFY_OUTPUT_DATA]/spark-warehouse/time_table/
5. songplays_table - partitionBy("year", "month"):
    - [AWS_SPARKIFY_OUTPUT_DATA]/spark-warehouse/songplays_table/

### ETL Details
A python program has been developed to extract the data from the given datasets present in S3 bucket into sets of parquet files after transforming the data using Spark.

### File List
1. dl.cfg - Configuration file which has the details of the AWS S3 buckets and path for the Song Data and Log data.
2. etl.py - ETL program. Log Data and Song data is read from S3 buckets, the data is transformed using the pySpark API and then stored in AWS S3 bucket as parquet files.

### Steps to create the tables and run the ETL process:
1. Update 'dwh.cfg' with the required details.
    - AWS_ACCESS_KEY_ID - Your AWS Acess Key ID
    - AWS_SECRET_ACCESS_KEY - Your AWS Secret Key
    - AWS_SPARKIFY_INPUT_DATA - S3 Bucket to read the Log and Song Data
    - AWS_SPARKIFY_OUTPUT_DATA - S3 Bucket to write the Parquet files into
    - SONG_DATA - Song Data path 
    - LOG_DATA - Log Data Path 
2. Execute 'etl.py'