-- DDL таблицы load_log для записи логов о загрузке данных в слой витрин из системы источника

-- drop table if exists VT260224AD30FB__DWH.load_log;

create table if not exists VT260224AD30FB__DWH.load_log
(
	schema_name varchar(100),
	table_name varchar(100),
	low_threshold timestamp,
	high_threshold timestamp,
	load_end timestamp,
	status varchar(20),  -- 'SUCCESS', 'ERROR'
	error_message varchar(1000)
)
order by load_end
segmented by hash(table_name, load_end) all nodes;