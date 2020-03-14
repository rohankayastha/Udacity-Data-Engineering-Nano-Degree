import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *


def process_song_file(cur, filepath):
    """
        This function processes the Song Dataset. The song file specified in the filepath is read and it's data inserted into the tables 'songs' and 'artists'.
        The input passed to this function are:
        1. Cursor variable 'cur' with the connection established to the Sparkify database.
        2. Absolute 'filepath' of the song json file to process.
        """    
    # open song file
    df = pd.read_json(filepath,typ='series')

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        This function processes the Log Dataset. The log file specified in the filepath is read and it's data inserted into the tables 'time', 'users' and 'songplays'
        The input passed to this function are:
        1. Cursor variable 'cur' with the connection established to the Sparkify database.
        2. Absolute 'filepath' of the Log json file to process.        
        """ 
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    time_data = []
    for i in t.index:
        list = t[i],t[i].hour,t[i].day,t[i].week,t[i].month,t[i].year,t[i].strftime('%a')
        time_data.append(list) 
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (datetime.datetime.fromtimestamp(row.ts/1000).strftime('%Y-%m-%d %H:%M:%S'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        This function processes the input datasets - Songs Dataset and Logs Dataset. Appropriate functions are called to read the data from the datasets and insert it into the Sparkify database.
        The input passed to this function are:
        1. Cursor variable 'cur' with the connection established to the Sparkify database.
        2. Connection variable 'conn' with the connection established to the Sparkify database.
        3. Filepath variable for the dataset to be processed.
        4. Fuction variable 'func' to process the specific dataset(Songs/Logs)
        """ 
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()