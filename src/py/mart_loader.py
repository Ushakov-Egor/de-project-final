from airflow.models import Variable
from airflow.providers.vertica.hooks.vertica import VerticaHook
from datetime import datetime, date, timedelta

import logging
from py.SqlHelper import SqlHelper

class MartLoader:
	def __init__(self, vertica: VerticaHook) -> None:
		# Хук соединения c Vertica из Airflow
		self._dwh = vertica
		self.cdm_schema = Variable.get('cdm_schema')        # Схема слоя витрин в DWH
		self.stg_schema = Variable.get('stg_schema')        # Схема стейнджинг-слоя в DWH
		# Создание объекта SqlHelper
		self.sqlhelper = SqlHelper(self._dwh)
		# Создание логгера
		self.logger = logging.getLogger(__name__)


	# Метод загрузки данных из слоя STG в слой витрин
	def load_mart_global_metrics(self, table_name: str) -> None:

		# Определение нижнего порога low_threshold интервала загрузки данных из stg в cdm
		with self._dwh.get_conn() as conn:
			with conn.cursor() as cursor:
				cursor.execute(f"SELECT MAX(date_update) FROM {self.cdm_schema}.{table_name};")
				low_threshold = cursor.fetchone()[0]
				if low_threshold is None:
					cursor.execute(f"SELECT MIN(date_update) FROM {self.stg_schema}.currencies;")
					low_threshold = cursor.fetchone()[0].date()
				else:
					low_threshold = low_threshold.date() + timedelta(days=1)
		
		# Верхний порог определяется по сегодняшней дате
		high_threshold = datetime.now().date() - timedelta(days=1)

		self.logger.info("Интервал загрузки данных из STG: %s - %s", low_threshold, high_threshold)

		# Загрузка данных в витрину
		sql = self.sqlhelper.load_sql('load_to_mart/load_mart_global_metrics.sql').format(table_name=f"{self.cdm_schema}.{table_name}")
		with self._dwh.get_conn() as conn:
			with conn.cursor() as cursor:
				try:
					cursor.execute(sql, parameters =  {"low_threshold": str(low_threshold), "high_threshold": str(high_threshold)})
					conn.commit()
					self.logger.info("Данные загружены в витрину %s за период: %s - %s", table_name, low_threshold, high_threshold)
					
					# Запись лога в DWH
					self.logger.info("Запись лога в DWH")
					load_end = datetime.now()
					status = "SUCCESS"
					error_message = None
					self.sqlhelper.write_dwh_log(self.cdm_schema,'load_log', low_threshold, high_threshold, table_name, load_end, status, error_message)
				except Exception as e:
					self.logger.error("Ошибка загрузки %s : %s", table_name, e)

					# Запись лога в DWH
					self.logger.info("Запись лога в DWH")
					load_end = datetime.now()
					status = "ERROR"
					error_message = str(e)
					self.sqlhelper.write_dwh_log(self.cdm_schema,'load_log', low_threshold, high_threshold, table_name, load_end, status, error_message)
					raise
