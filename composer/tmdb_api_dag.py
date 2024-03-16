from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from google.cloud import bigquery

def get_top_rated_movies():
    api_key = '8af774197ab9ae63aef54fc2df3f54ea'
    url = f'https://api.themoviedb.org/3/discover/movie?api_key={api_key}&language=en-US&sort_by=vote_average.desc&include_adult=false&include_video=false&page=1&vote_count.gte=1000'
    response = requests.get(url)
    if response.status_code == 200:
        movies_data = response.json()['results']
        return movies_data
    else:
        print('Erro ao fazer a solicitação:', response.status_code)


def insert_top_rated_movies_to_bigquery(**kwargs):
    movie_data = kwargs['ti'].xcom_pull(task_ids='get_top_rated_movies')

    client = bigquery.Client()

    table_id = 'proj_estudo.movies'  # Substitua pelo seu projeto e dataset

    # Prepara os dados para inserção no BigQuery
    rows_to_insert = []
    for movie in movie_data:
        row = {
            'id': movie.get('id', None),
            'title': movie.get('title', None),
            'overview': movie.get('overview', None),
            'release_date': movie.get('release_date', None),
            'backdrop_path': movie.get('backdrop_path', None),
            'belongs_to_collection': movie.get('belongs_to_collection', None),
            'budget': movie.get('budget', None),
            'genres': str(movie.get('genres', None)),
            'homepage': movie.get('homepage', None),
            'imdb_id': movie.get('imdb_id', None),
            'original_language': movie.get('original_language', None),
            'original_title': movie.get('original_title', None),
            'popularity': movie.get('popularity', None),
            'poster_path': movie.get('poster_path', None),
            'production_companies': str(movie.get('production_companies', None)),
            'production_countries': str(movie.get('production_countries', None)),
            'revenue': movie.get('revenue', None),
            'runtime': movie.get('runtime', None),
            'spoken_languages': str(movie.get('spoken_languages', None)),
            'status': movie.get('status', None),
            'tagline': movie.get('tagline', None),
            'video': movie.get('video', None),
            'vote_average': movie.get('vote_average', None),
            'vote_count': movie.get('vote_count', None)
        }
        rows_to_insert.append(row)

    # Insere os dados no BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if errors:
        print('Erros durante a inserção no BigQuery:', errors)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('tmdb_api_dag',
         default_args=default_args,
         description='DAG para obter detalhes dos 1000 filmes mais bem votados da API do TMDB e inserir no BigQuery via SQL',
         schedule_interval=None) as dag:

    get_top_rated_movies_task = PythonOperator(
        task_id='get_top_rated_movies',
        python_callable=get_top_rated_movies,
    )

    insert_top_rated_movies_to_bigquery_task = PythonOperator(
        task_id='insert_top_rated_movies_to_bigquery',
        python_callable=insert_top_rated_movies_to_bigquery,
    )

    get_top_rated_movies_task >> insert_top_rated_movies_to_bigquery_task
