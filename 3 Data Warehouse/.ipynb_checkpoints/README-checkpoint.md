# BUILD Database and ETL process to analyze Sparkify Data

#### Author
Rohan Pradhan

### Summary
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. This project deals with building ETL processes to load the data provided in a S3 bucket first to staging tables and then to set of Fact and Dimension tables. The data for analysis is stored in AWS Redshift.

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

### ETL Details
A python program has been developed to extract the data from the given datasets present in S3 bucket and load it first into stagin tables and then into the Fact and Dimension tables.

### File List
1. sql_queries.py - contains the SQLs to:
    - create/drop the database objects.
    - load the staging tables.
    - insert data into the final Fact/Dimension tables.
2. dwh.cfg - Configuration file which has the details related to the AWS Redshift cluster, AWS IAM Role and S3 Bucket where the source data is stored.
3. create_tables.py - program which is run to create the tables/sequences(drop them prior to creating the objects if they exist).
4. etl.py - ETL program. Data is first loaded into the staging tables and then into the fact/dimension tables.

### Steps to create the tables and run the ETL process:
1. Update dwh.cfg with the correct configuration parameters. The Cluster HOST and IAM ARN details are missing.
2. Execute 'create_tables.py'
3. Execute 'etl.py'