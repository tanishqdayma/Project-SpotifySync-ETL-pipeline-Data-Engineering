import pandas as pd 
import requests
from datetime import datetime
import datetime
import pandas as pd 
import requests
from datetime import datetime
import datetime
import access_token
from datetime import date


TOKEN = access_token.token


# Creating an function to be used in other pyrhon files
def return_artist_dataframe(): 
    input_variables = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
     
    #Download all songs from gloabl top 50 for the given day
    r = requests.get("https://api.spotify.com/v1/playlists/37i9dQZEVXbMDoHDwVN2tF", headers = input_variables)
    
    data_globaltop50 = r.json()

    artist_name = []
    artist_id = []
    track_name = []
    track_id = []

    # Extracting only the relevant bits of data from the json object      
    for i in data_globaltop50["tracks"]["items"]:
        artist_name.append(i["track"]["artists"][0]["name"])
        artist_id.append(i["track"]["artists"][0]["id"])
        track_name.append(i["track"]["name"])
        track_id.append(i["track"]["id"])
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    artist_dict = {
        "artist_name": artist_name,
        "artist_id" : artist_id,
        "track_name" : track_name,
        "track_id" : track_id
    }

    #Building the url to retrieve the Artist Information based on Artist IDs collecyed from the Global Top 50 Playlist
    artist_id_set = set(artist_id)
    artist_id_url = ""
    count=0
    for i in artist_id_set:
        if count == 0:
            artist_id_url = i
            count +=1
        else:
            artist_id_url = artist_id_url + "%2C" + i 

    r2 = requests.get("https://api.spotify.com/v1/artists?ids="+artist_id_url, headers=input_variables)

    data_artists_info = r2.json()

    art_name = []
    art_id = []
    total_followers = []
    genres = []
    image_url = []
    popularity = []

    for i in data_artists_info["artists"]:
        art_name.append(i["name"])
        art_id.append(i["id"])
        total_followers.append(i["followers"]["total"])
        genres.append(i["genres"])
        image_url.append(i["images"][0]["url"])
        popularity.append(i["popularity"])

    artist_info_dict = {
        "artist_name": art_name,
        "artist_id" : art_id,
        "total_followers" : total_followers,
        "genres" : genres,
        "image_url" : image_url,
        "popularity" : popularity
    }

    artist_df = pd.DataFrame(artist_dict, columns = ["artist_name", "artist_id", "track_name", "track_id"])

    artist_info_df = pd.DataFrame(artist_info_dict, columns=['artist_name', 'artist_id', 'total_followers', 'genres', 'image_url', 'popularity'])
    artist_info_df['genres'] = artist_info_df['genres'].apply(','.join)
     
    return artist_df, artist_info_df


def Data_Quality(load_df, df_name):
    #Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No Data Present in {}'.format(df_name))
        return False
    
    #Checking for Nulls in our data frame 
    if load_df.isnull().values.any():
        raise Exception("Null values found in {}".format(df_name))

# Writing some Transformation Queries to get the count of artist
def Count_tracks_df(load_df):

    #Applying logic
    count_tracks_df=load_df.groupby(['artist_id','artist_name'],as_index = False).count()
    count_tracks_df.rename(columns ={'track_name':'count'}, inplace=True)

    return count_tracks_df[['artist_id','artist_name','count']]


def spotify_etl():

    load_df=return_artist_dataframe()
    if(Data_Quality(load_df[0], "Global_50_Playlist") == False):
        raise ("Failed at Data Validation")
    if(Data_Quality(load_df[1], "Artist_Information") == False):
        raise ("Failed at Data Validation")
    count_tracks_df=Count_tracks_df(load_df[0])

    # Get today's date
    today = date.today()

    # Add a new column with today's date as the default value
    load_df[0]['playlist_date'] = today
    count_tracks_df['playlist_date'] = today

    return (load_df[0], load_df[1],count_tracks_df)

test = spotify_etl()

print(test[0])
print(test[1])
print(test[2])