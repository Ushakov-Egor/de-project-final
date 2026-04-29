from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import get_current_context

import logging
import pandas as pd
from datetime import date, datetime, timedelta
import io
from py.SqlHelper import SqlHelper

# Класс загрузки данных из источника Postgres в виде датафрейма Pandas
class Loader():
	def __init__(self, pg: PostgresHook, vertica: VerticaHook) -> None:
		# Хук соединения c Postgres из Airflow
		self._db = pg
		# Хук соединения c Vertica из Airflow
		self._dwh = vertica
		self.stg_schema = Variable.get('stg_schema')        # Схема стейнджинг-слоя в DWH
		self.src_schema = Variable.get('src_schema')        # Схема данных источника в Postgres
		# Создание логгера
		self.logger = logging.getLogger(__name__)
		# Создание объекта SqlHelper
		self.sqlhelper = SqlHelper(self._dwh)

	# Метод загрузки таблицы transactions из источника в датафрейм Pandas
	def get_transactions_data(self, table_name: str) -> pd.DataFrame:

		# Определение периода, за который нужно загрузить данные из источника transactions

		# Нижний порог определяется по DWH
		sql_max = self.sqlhelper.load_sql("load_from_src/get_max_transaction_dt.sql").format(table_name=f"{self.stg_schema}.{table_name}")     # sql для определения определения даты последней загруженной в DWH транзакции
		with self._dwh.get_conn() as conn:
			with conn.cursor() as cursor:
				cursor.execute(sql_max)
				low_threshold = cursor.fetchone()[0]
				# Если таблица в стейджинг-слое пустая, то азпрос вернет low_threshold = None, поэтому произведем проверку условием
				if low_threshold is None:
					low_threshold = date(1900, 1, 1)
				else:
					low_threshold = low_threshold.date() + timedelta(days=1)
		# Верхний порог определяется по контекстной дате Airflow
		context = get_current_context()
		end_date = context['data_interval_end']
		high_threshold = end_date.date()

		self.logger.info("Интервал загрузки данных из источника: %s - %s", low_threshold, high_threshold)

		# Загрузка данных из источника
		self.logger.info("Начинаю загрузку таблицы %s из источника", table_name)
		sql_source = self.sqlhelper.load_sql("load_from_src/get_transactions.sql").format(table_name=f"{self.src_schema}.{table_name}")     # sql для чтения данных из источника
		try:
			df = self._db.get_pandas_df(sql_source,
										parameters={"low": str(low_threshold), "high": str(high_threshold)})
			self.logger.info("Загружено %d строк из %s", len(df), table_name)
			return df
		except Exception as e:
			self.logger.error("Ошибка загрузки %s: %s", table_name, e)
			raise

	# Метод загрузки таблицы currencies из источника в датафрейм Pandas
	def get_currencies_data(self, table_name:str) -> pd.DataFrame:

		# Определение периода, за который нужно загрузить данные из источника currencies

		# Нижний порог определяется по DWH
		sql_max = self.sqlhelper.load_sql("load_from_src/get_max_currencies_dt.sql").format(table_name=f"{self.stg_schema}.{table_name}")     # sql для определения определения даты последней загруженной в DWH транзакции
		with self._dwh.get_conn() as conn:
			with conn.cursor() as cursor:
				cursor.execute(sql_max)
				low_threshold = cursor.fetchone()[0]
				# Если таблица в стейджинг-слое пустая, то азпрос вернет low_threshold = None, поэтому произведем проверку условием
				if low_threshold is None:
					low_threshold = date(1900, 1, 1)
				else:
					low_threshold = low_threshold.date() + timedelta(days=1)
		# Верхний порог определяется по контекстной дате Airflow
		context = get_current_context()
		end_date = context['data_interval_end']
		high_threshold = end_date.date()

		self.logger.info("Интервал загрузки данных из источника: %s - %s", low_threshold, high_threshold)

		# Загрузка данных из источника
		self.logger.info("Начинаю загрузку таблицы %s из источника", table_name)
		sql_source = self.sqlhelper.load_sql("load_from_src/get_currencies.sql").format(table_name=f"{self.src_schema}.{table_name}")     # sql для чтения данных из источника
		try:
			df = self._db.get_pandas_df(sql_source,
										parameters={"low": str(low_threshold), "high": str(high_threshold)})
			self.logger.info("Загружено %d строк из %s", len(df), table_name)
			return df
		except Exception as e:
			self.logger.error("Ошибка загрузки %s: %s", table_name, e)
			raise

	# этот метод загружает данные из датафрейма в хранилище в СУБД Vertica, для загрузки датафремй предварительно преобразуется в csv
	def load_data(self, df: pd.DataFrame, table_name: str) -> None:
		self.logger.info("Начинаю загрузку таблицы %s в хранилище", table_name)
		try:
			with self._dwh.get_conn() as conn:
				with conn.cursor() as cursor:
					# Создаём CSV в памяти из DataFrame
					csv_buffer = io.StringIO()
					df.to_csv(csv_buffer, sep='|', header=False, index=False, na_rep='\\N')
					csv_buffer.seek(0)

					# Копируем через STDIN
					sql_copy_to_dwh = self.sqlhelper.load_sql("load_from_src/copy_to_dwh.sql").format(table_name=f"{self.stg_schema}.{table_name}")
					cursor.copy(sql_copy_to_dwh,
							csv_buffer)
				conn.commit()
				self.logger.info("Загружено %d строк в %s", len(df), table_name)

				# Запись лога в DWH
				self.logger.info("Запись лога в DWH")
				load_end = datetime.now()
				rows_loaded = len(df)
				status = "SUCCESS"
				error_message = None
				self.sqlhelper.write_stg_log(self.stg_schema,'load_log', table_name, load_end, rows_loaded, status, error_message)

		except Exception as e:
			self.logger.error("Ошибка загрузки в хранилище %s: %s", table_name, e)

			# Запись лога в DWH
			self.logger.info("Запись лога в DWH")
			load_end = datetime.now()
			rows_loaded = 0
			status = "ERROR"
			error_message = str(e)
			self.sqlhelper.write_stg_log(self.stg_schema,'load_log', table_name, load_end, rows_loaded, status, error_message)

			raise