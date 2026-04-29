import config              # Необходимо для работы .py скриптов из папки /py

from airflow.decorators import dag, task
from airflow.providers.vertica.hooks.vertica import VerticaHook

from datetime import datetime
from py.mart_loader import MartLoader

@dag(
	dag_id='stg_to_mart',
	start_date=datetime(2024, 1, 1),
	schedule_interval='0 4 * * *',
	catchup=False,
	tags=['final_project']
)
def stg_to_mart_dag():

    vertica_hook = VerticaHook(vertica_conn_id ="DWH_conn")                  # Получаем информацию по соединениею с источником из Airflow для создание объекта класса Loader
    martloader = MartLoader(vertica_hook)                                    # Создание обекта класса MartLoader, в дальнейшем будет использоваться для загрузки данных в слой витрин данных 

    @task
    def load_mart_global_metrics():
        table_name = 'global_metrics'
        martloader.load_mart_global_metrics(table_name)
    load_mart_global_metrics()

stg_to_mart_dag()