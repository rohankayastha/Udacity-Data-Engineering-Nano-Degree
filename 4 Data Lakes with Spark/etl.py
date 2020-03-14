import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType,StringType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        This function creates a spark session. No parameters are passed to this method. This method returns the spark session object which has been created.
        """
    print("\n\nCreating Spark Session")
    start_time = datetime.now()
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    end_st = datetime.now()
    print("Spark Session created in {}.".format(end_st-start_time))
    return spark


def process_song_data(spark, input_data, output_data):
    """
        This method processes the song data read from the AWS S3 Bucket. The data is 
        stored in a Spark Dataframe. The data from this Spark Dataframe is then retreived
        using Spark SQL and then stored as per the requirement into separate Dataframes
        which are then written into parquet files.
        The input passed to this function are:
        1. Spark Session object
        2. AWS S3 Source Bucket name
        3. AWS S3 Destination Bucket name
        """
    # get filepath to song data file
    song_data = input_data+config['DATA']['SONG_DATA']
    
    # read song data file
    print("\n\nReading song data from {}".format(song_data))
    start_time = datetime.now()
    df = spark.read.json(song_data)
    end_st = datetime.now()
    print("Song data read in {} seconds.".format(end_st-start_time))

    # extract columns to create songs table    
    df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM songs
        order by song_id
        """)
    songs_table.dropDuplicates(['song_id'])
    print("\nsongs_table created with following structure:")
    songs_table.printSchema()
    print("Sample data in songs_table:")
    songs_table.show(5, truncate = False)
    
    # write songs table to parquet files partitioned by year and artist
    print("Writing songs_table data to parquet files in {}".format(output_data+"sparkify-warehouse/songs_table/"))
    start_time = datetime.now()
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data+"sparkify-warehouse/songs_table/")
    end_st = datetime.now()
    print("Song data written in {}.".format(end_st-start_time))

    # extract columns to create artists table    
    df.createOrReplaceTempView("artists")
    artists_table = spark.sql("""
        SELECT DISTINCT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
        FROM artists o
        WHERE song_id =
        (
            SELECT max(song_id) from artists i
            WHERE i.artist_id=o.artist_id
        )
        order by artist_id
        """)
    artists_table.dropDuplicates(['artist_id'])
    print("\n\nartists_table created with following structure:")
    artists_table.printSchema()
    print("Sample data in artists_table:")
    artists_table.show(5, truncate = False)
    
    # write artists table to parquet files
    print("Writing artists_table data to parquet files in {}".format(output_data+"sparkify-warehouse/artists_table/"))
    start_time = datetime.now()
    artists_table.write.mode("overwrite").parquet(output_data+"sparkify-warehouse/artists_table/")
    end_st = datetime.now()
    print("Song data written in {}.".format(end_st-start_time))


def process_log_data(spark, input_data, output_data):
    """
        This method processes the Log data read from the AWS S3 Bucket. The data is 
        stored in a Spark Dataframe. The data from this Spark Dataframe is then retreived
        using Spark SQL and then stored as per the requirement into separate Dataframes
        which are then written into parquet files.
        The input passed to this function are:
        1. Spark Session object
        2. AWS S3 Source Bucket name
        3. AWS S3 Destination Bucket name
        """
    # get filepath to log data file
    log_data = input_data+config['DATA']['LOG_DATA']

    # read log data file
    print("\n\nReading Log data from {}".format(log_data))
    start_time = datetime.now()
    df = spark.read.json(log_data)
    end_st = datetime.now()
    print("Log data read in {}.".format(end_st-start_time))
    
    # filter by actions for song plays
    df = df.filter("page='NextSong'")

    # extract columns for users table    
    df.createOrReplaceTempView("users")
    users_table = spark.sql("""
        SELECT DISTINCT userid,firstname,lastname,gender,level
        FROM users o
        WHERE userid IS NOT NULL
        and o.page='NextSong'
        AND ts =
        (
            select max(ts)
            from users i
            where o.userid = i.userid
        ) order by userid
        """)
    users_table.dropDuplicates(['userid'])
    print("\n\nusers_table created with following schema:")
    users_table.printSchema()
    print("Sample data in users_table:")
    users_table.show(5, truncate = False)
    
    # write users table to parquet files
    print("Writing users_table data to parquet files in {}".format(output_data+"sparkify-warehouse/users_table/"))
    start_time = datetime.now()
    users_table.write.mode("overwrite").parquet(output_data+"sparkify-warehouse/users_table/")
    end_st = datetime.now()
    print("User data written in {}.".format(end_st-start_time))

    # create timestamp column from original timestamp column
    def py_get_timestamp(input_ts):
        timestampvalue = datetime.fromtimestamp(input_ts/1000)
        return timestampvalue
    get_timestamp = udf(py_get_timestamp, TimestampType())
    
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    def py_get_datetime(input_ts):
        datetimevalue = datetime.fromtimestamp(input_ts/1000)
        return datetimevalue.strftime("%b %d %Y %H:%M:%S")
    get_datetime = udf(py_get_datetime, StringType())
    
    df = df.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    df.createOrReplaceTempView("time")
    time_table = spark.sql("""
        SELECT DISTINCT
        timestamp as start_time
        ,hour(timestamp) as hour
        ,day(timestamp) as day
        ,weekofyear(timestamp) as week
        ,month(timestamp) as month
        ,year(timestamp) as year
        ,dayofweek(timestamp) as weekday
        FROM time
        """)
    print("\n\ntime_table created with following schema:")
    time_table.dropDuplicates(['start_time'])
    time_table.printSchema()
    print("Sample data in time_table:")
    time_table.show(5, truncate = False)
    
    # write time table to parquet files partitioned by year and month
    print("Writing time_table data to parquet files in {}".format(output_data+"sparkify-warehouse/time_table/"))
    start_time = datetime.now()
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(output_data+"sparkify-warehouse/time_table/")
    end_st = datetime.now()
    print("Time data written in {}.".format(end_st-start_time))

    # read in song data to use for songplays table
    
    # get filepath to song data file
    song_data = input_data+config['DATA']['SONG_DATA']
    
    # read song data file
    print("\n\nReading song data from {}".format(song_data))
    start_time = datetime.now()
    song_df = spark.read.json(song_data)
    end_st = datetime.now()
    print("Song data read in {} seconds.".format(end_st-start_time))

    # extract columns from joined song and log datasets to create songplays table
    df.createOrReplaceTempView("logdata")
    song_df.createOrReplaceTempView("songdata")
    
    songplays_table = spark.sql("""
        SELECT row_number() over (order by "monotonically_increasing_id") as songplay_id,*
        from(
            SELECT DISTINCT
            l.timestamp as start_time
            ,l.userid
            ,l.level
            ,s.song_id
            ,s.artist_id
            ,l.sessionId
            ,l.location
            ,l.useragent
            ,year(timestamp) as year
            ,month(timestamp) as month
            FROM logdata l inner join songdata s
            on l.artist = s.artist_name and l.song = s.title)
        """)
    print("\n\nsongplays_table created with following strucuture:")
    songplays_table.printSchema()
    print("Sample data in songplays_table:")
    songplays_table.show(5, truncate = False)

    # write songplays table to parquet files partitioned by year and month
    print("Writing songplays_table data to parquet files in {}".format(output_data+"sparkify-warehouse/songplays_table/"))
    start_time = datetime.now()
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+"sparkify-warehouse/songplays_table/")
    end_st = datetime.now()
    print("Songplays data written in {}.".format(end_st-start_time))

def main():
    etl_start_time = datetime.now()
    print("ETL Started at {}".format(etl_start_time))
    spark = create_spark_session()
    input_data = config['AWS']['AWS_SPARKIFY_INPUT_DATA']
    output_data = config['AWS']['AWS_SPARKIFY_OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    etl_end_time = datetime.now()
    print("\n\n\nETL Completeded at {}".format(etl_end_time))
    print("Total time taken is {}".format(etl_end_time-etl_start_time))
    

if __name__ == "__main__":
    main()
