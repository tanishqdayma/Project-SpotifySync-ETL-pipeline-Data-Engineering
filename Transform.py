import Extract
import pandas as pd 

# Set of Data Quality Checks Needed to Perform Before Loading
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

    #Creating a Primary Key based on Timestamp and artist name
    # Transformed_df["ID"] = Transformed_df['timestamp'].astype(str) +"-"+ Transformed_df["artist_name"]

    return count_tracks_df[['artist_id','artist_name','count']]








    
    