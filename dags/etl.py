import sys
import os
import json
import logging as log
import pandas as pd
from dotenv import load_dotenv
from decouple import config

load_dotenv()
work_dir = os.getenv('WORK_DIR')
sys.path.append(work_dir)

from sqlalchemy import inspect, Table, MetaData, insert, select
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.exc import SQLAlchemyError
from googleapiclient.http import MediaInMemoryUpload
from googleapiclient.discovery import build
from google.oauth2 import service_account
from transforms.transform import *
from src.database_connection.dbconnection import getconnection
from src.model.models import *  
import io

def extract_grammy(**kwargs):
    engine = getconnection()
    Session = sessionmaker(bind=engine)
    session = Session()
    log.info("Doing data extraction of grammyAwards")
    table = aliased(GrammyAwards)
    query = str(session.query(table).statement)
    df = pd.read_sql(query, con=engine)
    log.info(f"Finished data extraction {df}")
    kwargs['ti'].xcom_push(key='grammy_data', value=df.to_json(orient='records'))
    return df.to_json(orient='records')



def transform_grammy(**kwargs):
    log.info("Doing Data transform")
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids="extract_grammy_task", key='grammy_data')
    if str_data is None:
        log.error("No data found to transform grammy")
        return

    json_df = json.loads(str_data)
    df = pd.json_normalize(data=json_df)
    log.info(f"Data is {df}")
    file = GrammyAwardsTransformer('./data/the_grammy_awards.csv')
    file.set_df(df)
    file.set_winners()
    file.set_artist_song_of_the_year()
    file.various_artist()
    df = file.df.copy()

    df_grouped = df.groupby(['nominee', 'artist','year']).agg(
        nomineeT=('nominee', 'size'),
        winner=('winner', 'sum')
    ).reset_index()

    df_grouped.columns = ['nominee', 'artist', 'year', 'nomineeT', 'winner']
    log.info('Aggregated DataFrame created')


    result = {
        "source": "grammy",
        "data": df_grouped.to_dict(orient='records')
    }
    kwargs['ti'].xcom_push(key='transformed_grammy_data', value=json.dumps(result))
    log.info('columns: ' + str(df_grouped.columns))
    return json.dumps(result)


def read_spotify(**kwargs):
    log.info("Doing Spotify extraction")
    df = pd.read_csv('data/spotify_dataset.csv')
    log.info('Spotify Dataset Read')
    kwargs['ti'].xcom_push(key='spotify_data', value=df.to_json(orient='records'))
    return df.to_json(orient='records')

def tranform_spotify(**kwargs):
    log.info("Starting Spotify transform")
    
    ti = kwargs["ti"]
    log.info("the kwargs are: ", kwargs)
    str_data = ti.xcom_pull(task_ids="extract_spotify_task", key='spotify_data')
    if str_data is None:
        log.error("No data found to transform spotify_data'")
        return

    json_df = json.loads(str_data)
    df = pd.json_normalize(data=json_df)
    log.info(f"Data is {df}")
    file = SpotifyDataTransformer(df)
    file.drop_na()
    file.drop_duplicates()
    log.info('Successfully transformed')
    result = {
        "source": "spotify",
        "data": file.df.to_dict(orient='records')
    }
    kwargs['ti'].xcom_push(key='spotify_data', value=json.dumps(result))

    return json.dumps(result)

def merge_data(**kwargs):
    log.info("Doing data merge")

    ti = kwargs["ti"]
    
    # Pull data from XCom
    json_grammy = ti.xcom_pull(task_ids="transform_grammy_task", key='transformed_grammy_data')
    json_spotify = ti.xcom_pull(task_ids="transform_spotify_task", key='spotify_data')

    log.info(json_grammy)
    if json_grammy is None:
        log.error("No data found for transform_grammy")
        return

    if json_spotify is None:
        log.error("No data found for transform_csv")
        return

    data_grammy = json.loads(json_grammy)
    data_spotify = json.loads(json_spotify)

    df_grammy = pd.DataFrame(data_grammy["data"])
    df_spotify = pd.DataFrame(data_spotify["data"])

    df_spotify['track_name'] = df_spotify['track_name'].str.lower()
    df_grammy['nominee'] = df_grammy['nominee'].str.lower()

    df_merged = df_spotify.merge(df_grammy, how='left', left_on='track_name', right_on='nominee')

    df_merged['nomineeT'] = df_merged['nomineeT'].fillna(0).astype(int)
    df_merged['winners'] = df_merged['winner'].fillna(0).astype(int)

    df_merged['max_popularity'] = df_merged.groupby('track_name')['popularity'].transform('max')
    df_merged.loc[df_merged['popularity'] < df_merged['max_popularity'], ['nomineeT', 'winner']] = 0, 0
    df_merged = df_merged.drop('max_popularity', axis=1) 
    
    log.info(f"Merged DataFrame shape: {df_merged.shape}")
    log.info(f"Merged DataFrame columns: {df_merged.columns}")

    kwargs['ti'].xcom_push(key='merge_data', value=df_merged.to_json(orient='records'))

    return df_merged.to_json(orient='records')


def load_merged_data(**kwargs):
    log.info("loading merged data")

    ti = kwargs['ti']
    json_df = ti.xcom_pull(task_ids="merge_task", key='merge_data')  
    if json_df is None:
        log.error("No data found for merge_task")  
        return

    df = pd.DataFrame(json.loads(json_df))
    log.info(df)
    df.drop(columns=['artist','nominee'], inplace=True)
    engine = getconnection()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        if inspect(engine).has_table('Songsdata'):
            Songsdata.__table__.drop(engine)
        Songsdata.__table__.create(engine)
        log.info("Table created successfully.")


    except SQLAlchemyError as e:
        log.error(f"Error creating table: {e}")
        return 

    try:
        df.drop_duplicates(subset='id', inplace=True)
        log.info(df.shape)
        df.to_sql('Songsdata', con=engine, if_exists='append', index=False)
        log.info('Data loaded successfully')
        kwargs['ti'].xcom_push(key='loaded_data', value=df.to_json(orient='records'))
        return df.to_json(orient='records') 
    except Exception as e:
        log.error(f"Error loading data: {e}") 
        return None


def store_data(**kwargs):
    log.info('starting data store')
    ti = kwargs['ti']
    json_df = ti.xcom_pull(task_ids="load_task", key='loaded_data')  
    if json_df is None:
        log.error("No data found  for load_task") 
        return

    df = pd.DataFrame(json.loads(json_df))


    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = './drive/driveapi.json'   
    PARENT_FOLDER_ID = "1h6cLnXLtk1ZTcV-tGgDGQ5bKd_HF4l93"
    

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    file_metadata = {
        'name': 'SpotifyGrammy.csv',
        'parents': [PARENT_FOLDER_ID],
        'mimeType': 'text/csv'
    }

    csv_bytes = csv_buffer.getvalue().encode('utf-8')
    media = MediaInMemoryUpload(csv_bytes, mimetype='text/csv')

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

