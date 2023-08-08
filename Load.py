import Extract
import Transform
import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///GlobalTop50Tracks.sqlite"

if __name__ == "__main__":

#Importing the songs_df from the Extract.py
    load_df=Extract.return_artist_dataframe()
    if(Transform.Data_Quality(load_df[0], "Global_50_Playlist") == False):
        raise ("Failed at Data Validation")
    if(Transform.Data_Quality(load_df[1], "Artist_Information") == False):
        raise ("Failed at Data Validation")
    count_tracks_df=Transform.Count_tracks_df(load_df[0])
    #The Three Data Frames that need to be Loaded in to the DataBase

#Loading into Database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('GlobalTop50Tracks.sqlite')
    cursor = conn.cursor()

    #SQL Query to Create A Table with 50 tracks
    sql_query_1 = """
    CREATE TABLE IF NOT EXISTS Top50Tracks(
        artist_id VARCHAR(200),
        artist_name VARCHAR(200),
        track_id VARCHAR(200),
        track_name VARCHAR(200),
        playlist_date DATE,
        CONSTRAINT primary_key_constraint PRIMARY KEY (track_id, playlist_date)
    )
    """
    #SQL Query to Create A Table with Artist Information
    sql_query_2 = """
    CREATE TABLE IF NOT EXISTS Artist_Info(
        artist_name VARCHAR(200),
        artist_id VARCHAR(200),
        total_followers BIGINT,
        genres TEXT,
        image_url VARCHAR(500),
        popularity INT(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (artist_id)
    )
    """
    #SQL Query to Create A Table with Most Listened Artist
    sql_query_3 = """
    CREATE TABLE IF NOT EXISTS Track_Count_per_Artist(
        artist_id VARCHAR(200),
        artist_name VARCHAR(200),
        count INT,
        playlist_date DATE,
        CONSTRAINT primary_key_constraint PRIMARY KEY (artist_id)
    )
    """

    # Get today's date
    today = date.today()

    # Add a new column with today's date as the default value
    load_df[0]['playlist_date'] = today
    count_tracks_df['playlist_date'] = today

    cursor.execute(sql_query_1)
    cursor.execute(sql_query_2)
    cursor.execute(sql_query_3)
    print("Opened database successfully")

    #We need to only Append New Data to avoid duplicates
    try:
        load_df[0].to_sql("Top50Tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the Top50Tracks table")
    try:
        load_df[1].to_sql("Artist_Info", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the Artist_Info table")
    try:
        count_tracks_df.to_sql("Track_Count_per_Artist", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the Track_Count_per_Artist table")

    # cursor.execute('DROP TABLE Top50Tracks')
    # cursor.execute('DROP TABLE Artist_Info')
    # cursor.execute('DROP TABLE Track_Count_per_Artist')

    conn.close()
    print("Close database successfully")
    
    