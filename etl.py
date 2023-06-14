import os
import glob
import psycopg2
import datetime
import pandas as pd
from pandas import Series, DataFrame
from sql_queries import *


def process_song_file(cur, filepath):
    """
    
    - Processes a single song file    
    - Inserts the record into DB
       
    """
    # open song file
    df = pd.read_json(filepath, typ = Series)

    # insert song record
    song_data = [
        df.values[6],
        df.values[7],
        df.values[1],
        df.values[9],
        df.values[8]   
    ]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [
        df.values[1],
        df.values[5],
        df.values[4],
        df.values[2],
        df.values[3],
    ]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    
    - Processes a single log file    
    - Inserts the time data records into DB by filtering with NextSong and converting timestamp column to datetime
    - Inserts user data and songplay data
    
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t.astype('datetime64').values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    items = {}
    for index, i in enumerate(time_data):
        items[column_labels[index]]=i
        
    time_df = pd.DataFrame(items)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songId, artistId = results
            # insert songplay record
            songplay_data = [datetime.datetime.fromtimestamp(row.ts / 1e3), row.userId, row.level, songId, artistId, row.sessionId, row.location, row.userAgent]
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    
    - Processes all files with .json extension    
    
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
    """
    
    - Creates and connects to the sparkifydb
    - Process the song data and log data
    
    """
    conn = psycopg2.connect("")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()