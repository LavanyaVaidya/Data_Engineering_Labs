from datetime import datetime, timedelta
from airflow import DAG

# Airflow 2 imports
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator  # replaces DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


import pandas as pd
import boto3
import logging


# -------------------------
# Default args (Airflow 2)
# -------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -------------------------
# DAG-level constants
# -------------------------
BUCKET_NAME = 'lava-aws-de-labs'
SONGS_FILE_PATH = 'spotify_data/songs.csv'
USERS_FILE_PATH = 'spotify_data/users.csv'
STREAMS_PREFIX = 'spotify_data/streams/'
ARCHIVE_PREFIX = 'spotify_data/streams/archived/'

REQUIRED_COLUMNS = {
    'songs': [
        'id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity',
        'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness',
        'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness',
        'valence', 'tempo', 'time_signature', 'track_genre'
    ],
    'streams': ['user_id', 'track_id', 'listen_time'],
    'users': ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
}


# -------------------------
# Helper functions
# -------------------------
def list_s3_files(prefix, bucket=BUCKET_NAME):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [
        obj['Key']
        for obj in response.get('Contents', [])
        if obj['Key'].endswith('.csv')
    ]


def read_s3_csv(file_name, bucket=BUCKET_NAME):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=file_name)
    return pd.read_csv(obj['Body'])


def validate_datasets():
    validation_results = {}

    songs = read_s3_csv(SONGS_FILE_PATH)
    validation_results['songs'] = set(REQUIRED_COLUMNS['songs']).issubset(songs.columns)

    users = read_s3_csv(USERS_FILE_PATH)
    validation_results['users'] = set(REQUIRED_COLUMNS['users']).issubset(users.columns)

    stream_files = list_s3_files(STREAMS_PREFIX)
    validation_results['streams'] = True
    for f in stream_files:
        streams = read_s3_csv(f)
        if not set(REQUIRED_COLUMNS['streams']).issubset(streams.columns):
            validation_results['streams'] = False
            break

    return validation_results


def branch_task(ti):
    results = ti.xcom_pull(task_ids='validate_datasets')
    return 'calculate_genre_level_kpis' if all(results.values()) else 'end_dag'


def upsert_to_redshift(df, table_name, id_columns):
    hook = PostgresHook(postgres_conn_id="redshift_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        data = [tuple(x) for x in df.to_numpy()]
        cols = ', '.join(df.columns)
        vals = ', '.join(['%s'] * len(df.columns))

        cursor.executemany(
            f"INSERT INTO reporting_schema.tmp_{table_name} ({cols}) VALUES ({vals})",
            data
        )

        delete_condition = ' AND '.join(
            [f"tmp_{table_name}.{c} = {table_name}.{c}" for c in id_columns]
        )

        cursor.execute(f"""
            BEGIN;
            DELETE FROM reporting_schema.{table_name}
            USING reporting_schema.tmp_{table_name}
            WHERE {delete_condition};

            INSERT INTO reporting_schema.{table_name}
            SELECT * FROM reporting_schema.tmp_{table_name};

            TRUNCATE TABLE reporting_schema.tmp_{table_name};
            COMMIT;
        """)
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def calculate_genre_level_kpis():
    streams = pd.concat([read_s3_csv(f) for f in list_s3_files(STREAMS_PREFIX)])
    songs = read_s3_csv(SONGS_FILE_PATH)

    streams['listen_date'] = pd.to_datetime(streams['listen_time']).dt.date
    merged = streams.merge(songs, on='track_id')

    genre_counts = merged.groupby(['listen_date', 'track_genre']).size().reset_index(name='listen_count')
    total = merged.groupby('listen_date').size().reset_index(name='total_listens')

    final = genre_counts.merge(total, on='listen_date')
    final['popularity_index'] = final['listen_count'] / final['total_listens']

    upsert_to_redshift(final, 'genre_level_kpis', ['listen_date', 'track_genre'])


def calculate_hourly_kpis():
    streams = pd.concat([read_s3_csv(f) for f in list_s3_files(STREAMS_PREFIX)])
    songs = read_s3_csv(SONGS_FILE_PATH)
    users = read_s3_csv(USERS_FILE_PATH)

    streams['listen_date'] = pd.to_datetime(streams['listen_time']).dt.date
    streams['listen_hour'] = pd.to_datetime(streams['listen_time']).dt.hour

    full = streams.merge(songs, on='track_id').merge(users, on='user_id')

    hourly = full.groupby(['listen_date', 'listen_hour'])['user_id'].nunique().reset_index(name='unique_listeners')

    upsert_to_redshift(hourly, 'hourly_kpis', ['listen_date', 'listen_hour'])


def move_processed_files():
    s3 = boto3.client('s3')
    for file in list_s3_files(STREAMS_PREFIX):
        dest = file.replace('spotify_data/streams/', 'spotify_data/streams/archived/')
        s3.copy_object(
            CopySource={'Bucket': BUCKET_NAME, 'Key': file},
            Bucket=BUCKET_NAME,
            Key=dest
        )
        s3.delete_object(Bucket=BUCKET_NAME, Key=file)


# -------------------------
# DAG definition (Airflow 2)
# -------------------------
with DAG(
    dag_id='data_validation_and_kpi_computation',
    start_date=datetime(2024, 1, 1),   # ✅ explicit start_date
    schedule='@daily',                # ✅ schedule instead of schedule_interval
    catchup=False,
    default_args=default_args,
) as dag:

    validate = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_datasets
    )

    branch = BranchPythonOperator(
        task_id='check_validation',
        python_callable=branch_task
    )

    genre_kpis = PythonOperator(
        task_id='calculate_genre_level_kpis',
        python_callable=calculate_genre_level_kpis
    )

    hourly_kpis = PythonOperator(
        task_id='calculate_hourly_kpis',
        python_callable=calculate_hourly_kpis
    )

    move_files = PythonOperator(
        task_id='move_processed_files',
        python_callable=move_processed_files
    )

    end = EmptyOperator(task_id='end_dag')

    validate >> branch >> [genre_kpis, end]
    genre_kpis >> hourly_kpis >> move_files
