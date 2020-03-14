# BUILD Database and ETL process to analyze Sparkify Data

#### Author
Rohan Pradhan

### Summary
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. This project deals with building ETL processes to load the gathered data in a Postgres database for their analysis.

### Sources of Data
1. Songs dataset: Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.
2. Log Dataset: Consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. The log files are partitioned by year and month

### Schema details
A star schema has been designed which consists of the following Fact and Dimension tables. The advantages of using a star schema design are:
1. Query performance - Because a star schema database has a small number of tables and clear join paths, queries run faster than they do against an OLTP system.
2. Load performance and administration - Structural simplicity also reduces the time required to load large batches of data into a star schema database. By defining facts and dimensions and separating them into different tables, the impact of a load operation is reduced. Dimension tables can be populated once and occasionally refreshed. You can add new facts regularly and selectively by appending records to a fact table.
3. Built-in referential integrity - A star schema has referential integrity built in when data is loaded.
4. Easily understood - A star schema is easy to understand and navigate, with dimensions joined only through the fact table. 

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

#### Sequence:
1. a sequence has been created to populate the primary key for the table songplays. (Note- Identity column has not been used as it is supported on from v10 onwards)

### ETL Details
A python program has been developed to extract the data from the given datasets and load it into the Fact and Dimension tables after transforming it where required.

### File List
1. Data (folder) - contains the data acting as input to the ETL process.
2. sql_queries.py - contains the SQLs to create/drop the database objects.
3. create_tables.py - program which is run to create the tables/sequences(drop them prior to creating the objects if they exist).
4. etl.py - ETL program. JSON files present in the 'Data' folder is the input for the this program. Data is transformed wherever required and loaded into the Postgres database.
5. etl.ipynb - Jupyter notebook to develop/test the code for creating the ETL program.
6. test.ipynb - Jupyter notebook to test whether the database objects have been created and view the sample data in the tables.

### Steps to create the tables and run the ETL process:
1. Execute 'create_tables.py'
2. Execute 'etl.py'