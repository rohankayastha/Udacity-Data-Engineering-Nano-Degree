import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
 artist VARCHAR
,auth VARCHAR
,firstname VARCHAR
,gender CHAR(1)
,iteminsession INT
,lastname VARCHAR
,length decimal(20,10)
,level VARCHAR
,location VARCHAR
,method VARCHAR
,page VARCHAR
,registration BIGINT
,sessionid INT
,song VARCHAR
,status INT
,start_time BIGINT
,useragent VARCHAR
,userid INT
);

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
 num_songs INT NOT NULL
,artist_id CHAR(18)
,artist_latitude float
,artist_longitude float
,artist_location VARCHAR
,artist_name VARCHAR
,song_id CHAR(18)
,title VARCHAR
,duration decimal(20,10)
,year INT
);

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
 songplay_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL
,start_time TIMESTAMP NOT NULL
,user_id INT NOT NULL
,level VARCHAR
,song_id CHAR(18) NOT NULL
,artist_id CHAR(18)
,session_id INT
,location VARCHAR
,user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
 user_id INT PRIMARY KEY NOT NULL
,first_name VARCHAR
,last_name VARCHAR
,gender CHAR(1)
,level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
 song_id CHAR(18) PRIMARY KEY NOT NULL
,title VARCHAR
,artist_id CHAR(18)
,year INT
,duration decimal(20,10)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
 artist_id CHAR(18) PRIMARY KEY NOT NULL
,name VARCHAR
,location VARCHAR
,latitude FLOAT(4)
,longitude FLOAT(4)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
 start_time TIMESTAMP PRIMARY KEY NOT NULL
,hour INT NOT NULL
,day INT NOT NULL
,week INT NOT NULL
,month INT NOT NULL
,year INT NOT NULL
,weekday CHAR(3) NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON {};
""").format(config.get('S3', 'LOG_DATA'),config.get('IAM_ROLE', 'ARN'),config.get('S3', 'LOG_JSONPATH'))


staging_songs_copy = ("""
copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 'auto' truncatecolumns;
""").format(config.get('S3', 'SONG_DATA'),config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays( start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT  DISTINCT timestamp 'epoch' + e.start_time/1000 * interval '1 second',e.userid,e.level,s.song_id,s.artist_id,e.sessionid,e.location,e.useragent from staging_events e, staging_songs s
where e.artist = s.artist_name and e.song = s.title and e.page='NextSong'
;
""")

# The source data for the users table is the staging_events table. The staging_events table may have multiple rows for the same userid if the 'level' has changed.
# The below SQL only inserts a single row in the users table with the latest 'level'.
user_table_insert = ("""
INSERT INTO users(user_id,first_name,last_name,gender,level)
SELECT DISTINCT userid,firstname,lastname,gender,level
FROM staging_events o
WHERE userid IS NOT NULL
and o.page='NextSong'
AND start_time =
(
  select max(start_time)
  from staging_events i
  where o.userid = i.userid
) order by userid
;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,title,artist_id,year,duration FROM staging_songs;
""")

# The source data for the users table is the staging_songs table. The staging_songs table may have multiple rows for the same artist_id.
# The below SQL only inserts a single row in the artists table.
artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
FROM staging_songs o
WHERE song_id =
(
  SELECT max(song_id) from staging_songs i
  WHERE i.artist_id=o.artist_id
)
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
  start_time
  ,cast(date_part(HOUR, start_time) as INT)
  ,cast(date_part(DAY, start_time) as INT)
  ,cast(date_part(WEEK, start_time) as INT)
  ,cast(date_part(MONTH, start_time) as INT)
  ,cast(date_part(YEAR, start_time) as INT)
  ,case
    when date_part(weekday, start_time) = 0
      then 'SUN'
    when date_part(weekday, start_time) = 1
      then 'MON'
    when date_part(weekday, start_time) = 2
      then 'TUE'
    when date_part(weekday, start_time) = 3
      then 'WED'
    when date_part(weekday, start_time) = 4
      then 'THU'
    when date_part(weekday, start_time) = 5
      then 'FRI'
    when date_part(weekday, start_time) = 6
      then 'SAT'            
    else 'Other'
  end
FROM (select timestamp 'epoch' + start_time/1000 * interval '1 second' AS start_time from staging_events);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
