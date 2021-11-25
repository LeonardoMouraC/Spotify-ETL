from datetime import date, datetime
import json


import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pandas as pd
import requests
import datetime
import sqlite3 

DATABASE ="sqlite:///recently_played_tracks.sqlite"
USER_ID = '22b5pgubqoiuech6fptc76zaq'
TOKEN = 'BQBVjXy-OS31BbajF6L_1i7b0WWz3iyndiPTRMQmPL0Xi9xJ5Q6XsD7Civq6adOdDliMlH1WsIbOGWPN5dt0FJd1mHyWgPjIBp48wglNgMf6OZUpfXet7gg2be4fN7S_WaYcLGem2GDGXhkKCWWlfVag4gSLUKFlFINI0QXZ'

def validate_data(df: pd.DataFrame) -> bool:

    # Check if dataframe is empty
    if df.empty:
        print('No songs downloaded. Finishing execution')
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception('Primary Key were violated')

    # Check for nulls
    if df.isnull().values.any():
        nan_columns = df.columns[df.isna().any()].tolist()
        raise Exception(f'Null values found in {nan_columns}')

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True



if __name__ =='__main__':
    headers = {
        'Accept' : 'application/json',
        'Content-Type' : 'application/json',
        'Authorization' : f'Bearer {TOKEN}'   
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(f'https://api.spotify.com/v1/me/player/recently-played?limit=50&after={yesterday_unix_timestamp}', headers = headers)

    data = r.json()

    song_names = []
    artist_name = []
    played_at = []
    timestamp = [] 

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_name.append(song['track']['album']['artists'][0]['name'])
        played_at.append(song['played_at'])
        timestamp.append(song['played_at'][0:10])
    
    song_dict = {
        'song_name' : song_names,
        'artist_name' : artist_name,
        'played_at' : played_at,
        'timestamp' : timestamp
    }

    song_df = pd.DataFrame(song_dict, columns=['song_name', 'artist_name', 'played_at', 'timestamp'])

    print(song_df)

    #Validatiom
    if validate_data:
        print('Valid data, proceed to load')

    engine = sqlalchemy.create_engine(DATABASE)
    conn = sqlite3.connect('recently_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS recently_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        time_stamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(sql_query)
    print("Opened database succesfuly")

    try:
        song_df.to_sql('recently_played_tracks', engine, index=False, if_exists='append' )
    except: ('data already exists')

    print('Data was sucessfuly inserted')

    conn.close()
    print('Close connection with database')