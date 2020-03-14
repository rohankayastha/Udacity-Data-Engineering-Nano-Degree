# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
songplay_sequence_drop = "DROP SEQUENCE IF EXISTS songplay_id_seq"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
 songplay_id INT PRIMARY KEY NOT NULL
,start_time TIMESTAMP NOT NULL
,user_id INT
,level VARCHAR
,song_id CHAR(18)
,artist_id CHAR(18)
,session_id INT
,location VARCHAR
,user_agent VARCHAR
);
""")

# CREATE SEQUENCE to populate songplay_id
songplay_sequence_create = ("CREATE SEQUENCE IF NOT EXISTS songplay_id_seq START 1")

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

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES(nextval('songplay_id_seq'),%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (user_id)
DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (song_id)
DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT (artist_id)
DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
VALUES(%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (start_time)
DO NOTHING
""")

# FIND SONGS

song_select = ("""
SELECT
    songs.song_id,
    songs.artist_id
FROM 
    songs INNER JOIN artists
        on songs.artist_id = artists.artist_id
WHERE
    songs.title = %s
    and artists.name = %s
    and songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_sequence_queries = [songplay_sequence_create]
drop_sequence_queries = [songplay_sequence_drop]