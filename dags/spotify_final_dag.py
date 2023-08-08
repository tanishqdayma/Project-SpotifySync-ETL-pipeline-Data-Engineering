import datetime as date
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2

#from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine


from spotify_etl import spotify_etl



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': date.datetime(2023,7,12),
    #'retries': 1,
    #'retry_delay': date.timedelta(minutes=1)
}


def ETL():
    print("started")
    load_df=spotify_etl()
    #print(df)
    conn = BaseHook.get_connection('postgre_sql')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    conn2 = engine.connect()
    # df.to_sql('my_played_tracks', engine, if_exists='replace')
    

    load_df[0].to_sql('top50tracks', con=conn2,index=False, if_exists='append')
    load_df[2].to_sql('track_count_per_artist', con=conn2,index=False, if_exists='append')
    #load_df[1].to_sql('artist_info', con=conn2,index=False, if_exists='append')
    conn3 = psycopg2.connect(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    conn3.autocommit = True
    cursor = conn3.cursor()
    insert_query = f'''INSERT INTO artist_info(artist_name,artist_ID,total_followers,genres,image_url,popularity) VALUES 
        {','.join([str(i) for i in list(load_df[1].to_records(index=False))])} ON CONFLICT (artist_ID) DO NOTHING;'''
    cursor.execute(insert_query)



with DAG("spotify_final_dag", default_args=default_args,
    schedule_interval="@daily",description='Spotify ETL process 1-day', catchup=False) as dag:
    create_table_Top50Tracks= PostgresOperator(
        task_id='create_table_Top50Tracks',
        postgres_conn_id='postgre_sql',
        #schema='spotify_db',
        sql="""
        CREATE TABLE IF NOT EXISTS Top50Tracks(
        artist_id VARCHAR(200),
        artist_name VARCHAR(200),
        track_id VARCHAR(200),
        track_name VARCHAR(200),
        playlist_date DATE,
        PRIMARY KEY (track_id,playlist_date)); """
    )
    
    create_table_Artist_Info= PostgresOperator(
        task_id='create_table_Artist_Info',
        postgres_conn_id='postgre_sql',
        #schema='spotify_db',
        sql="""
        CREATE TABLE IF NOT EXISTS Artist_Info(
        artist_name VARCHAR(200),
        artist_ID VARCHAR(200),
        total_followers bigint,
        genres VARCHAR(200),
        image_url VARCHAR(500),
        popularity int,
        PRIMARY KEY (artist_ID));"""
    )

    create_table_TrackCount= PostgresOperator(
        task_id='create_table_TrackCount',
        postgres_conn_id='postgre_sql',
        #schema='spotify_db',
        sql="""
        CREATE TABLE IF NOT EXISTS Track_Count_per_Artist(
        artist_ID VARCHAR(200),
        artist_name VARCHAR(200),
        count int,
        playlist_date DATE,
        PRIMARY KEY (artist_ID,playlist_date));"""
    )



    run_etl = PythonOperator(
        task_id='spotify_etl_final',
        python_callable=ETL
    )

    [create_table_Top50Tracks,create_table_Artist_Info,create_table_TrackCount] >> run_etl
