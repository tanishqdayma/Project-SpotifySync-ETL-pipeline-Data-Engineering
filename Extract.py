import pandas as pd 
import requests
from datetime import datetime
import datetime
import access_token


# USER_ID = "31dqzuboa2ca62wbgcz3rsa4dndu" 
TOKEN = access_token.token

# Creating an function to be used in other pyrhon files
def return_artist_dataframe(): 
    input_variables = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
     
    
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
    # artist_df.drop_duplicates(inplace=True)
    # artist_df["id"] = range(1, len(artist_df) + 1)

    artist_info_df = pd.DataFrame(artist_info_dict, columns=['artist_name', 'artist_id', 'total_followers', 'genres', 'image_url', 'popularity'])
    artist_info_df['genres'] = artist_info_df['genres'].apply(','.join)
    # artist_info_df["id"] = range(1, len(artist_info_df) + 1)
  
    
    return artist_df, artist_info_df

# test_df = return__artist_dataframe()
