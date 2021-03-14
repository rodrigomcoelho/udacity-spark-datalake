import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, first, asc, desc
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType
from pyspark.sql.types import TimestampType, DateType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Creates a spark session connection
    """

    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Obtains song data from json files and processes
        them using spark, writing data to songs_table and
        artists_table parquet files

        Keyword arguments
        spark - spark connection
        input_data - path to read data from
        output_data - path to write parquet files to
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # define schema for reading
    tableSchema = StructType([
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_name', StringType()),
        StructField('duration', DoubleType()),
        StructField('num_songs', IntegerType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('year', IntegerType())
    ])

    # read song data file
    df = spark.read.option('multiline', 'true') \
                   .option('mergeSchema', 'true') \
                   .option('recursiveFileLookup', 'true') \
                   .json(song_data, schema=tableSchema)

    # extract columns to create songs table
    songs_table = df.dropna(how='any', subset=['artist_id', 'song_id']) \
                    .select(['song_id', 'title', 'artist_id',
                             'year', 'duration']) \
                    .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite') \
        .partitionBy('year', 'artist_id') \
        .parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df.dropna(how='any', subset=['artist_id']) \
                      .select(col('artist_id'),
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('lattitude'),
                              col('artist_longitude').alias('longitude'))\
                      .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode('overwrite') \
        .parquet(output_data + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    """
        Obtains log data from json files and processes
        them using spark, writing data to users_table,
        time_table, and songplays_table parquet files

        Keyword arguments
        spark - spark connection
        input_data - path to read data from
        output_data - path to write parquet files to
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # define schema to log
    logSchema = StructType([
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('itemInSession', LongType()),
        StructField('lastName', StringType()),
        StructField('length', DoubleType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', DoubleType()),
        StructField('sessionId', LongType()),
        StructField('song', StringType()),
        StructField('status', IntegerType()),
        StructField('ts', LongType()),
        StructField('userAgent', StringType()),
        StructField('userId', StringType())
    ])

    # read log data file
    df = spark.read \
              .option('multiline', 'true') \
              .option('mergeSchema', 'true') \
              .option('recursiveFileLookup', 'true') \
              .json(log_data, schema=logSchema)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    df = df.withColumn('userId', col('userId').cast(IntegerType()))

    # extract columns for users table
    users_table = df.groupBy(col('userId').alias('user_id')) \
                    .agg(first('firstName').alias('first_name'),
                         first('lastName').alias('last_name'),
                         first('gender').alias('gender'),
                         first('level').alias('level'))

    # write users table to parquet files
    users_table.write.mode('overwrite') \
        .partitionBy('gender') \
        .parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0),
                        TimestampType())
    df = df.withColumn('timestamp', get_timestamp('ts'))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), DateType())
    df = df.withColumn('datetime', get_datetime('ts'))

    # extract columns to create time table
    time_table = df.select(date_format('timestamp', 'yyyy-MM-dd HH:mm:ss')
                           .alias('start_time')
                           .cast(TimestampType()),
                           hour('timestamp').alias('hour'),
                           dayofmonth('timestamp').alias('day'),
                           weekofyear('timestamp').alias('week'),
                           month('timestamp').alias('month'),
                           year('timestamp').alias('year'))\
                   .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(output_data + 'time.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs.parquet')
    artist_df = spark.read.parquet(output_data + 'artists.parquet')
    joined_df = song_df.join(artist_df,
                             song_df.artist_id == artist_df.artist_id) \
                       .drop(song_df.artist_id) \
                       .select('song_id', 'title', 'name',
                               'duration', 'artist_id')

    # extract columns from joined song and log datasets
    # to create songplays table
    songplays_table = df.join(joined_df, (df.song == joined_df.title) &
                                         (df.artist == joined_df.name) &
                                         (df.length == joined_df.duration),
                              how='left')

    songplays_table = songplays_table.withColumn('songplay_id',
                                                 monotonically_increasing_id())

    songplays_table = songplays_table.select(col('songplay_id'),
                                             date_format('timestamp',
                                                         'yyyy-MM-dd HH:mm:ss')
                                             .alias('start_time')
                                             .cast(TimestampType()),
                                             col('userId').alias('user_id'),
                                             col('level'),
                                             col('song_id'),
                                             col('artist_id'),
                                             col('sessionId')
                                             .alias('session_id'),
                                             col('location'),
                                             col('userAgent')
                                             .alias('user_agent'),
                                             year(col('timestamp'))
                                             .alias('year'),
                                             month(col('timestamp'))
                                             .alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(output_data + 'songplays.parquet')


def main():
    spark = create_spark_session()

    input_data = config['S3']['INPUT_PATH']
    output_data = config['S3']['OUTPUT_PATH']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
