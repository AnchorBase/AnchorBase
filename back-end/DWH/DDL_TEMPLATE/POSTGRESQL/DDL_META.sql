
--устаревший файл
--Создание схем
-- DROP SCHEMA am

CREATE SCHEMA am;

-- DROP SCHEMA idmap;

CREATE SCHEMA idmap ;

-- DROP SCHEMA meta;

CREATE SCHEMA meta ;

-- DROP SCHEMA stg;

CREATE SCHEMA stg ;

CREATE SCHEMA wrk ;

CREATE EXTENSION "uuid-ossp";


--Создание таблиц метаданных
-- Drop table

-- DROP TABLE meta.anchor_column;

CREATE TABLE meta.anchor_column (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.anchor_table;

CREATE TABLE meta.anchor_table (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.dwh;

CREATE TABLE meta.dwh (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.entity;

CREATE TABLE meta.entity (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.entity_attribute;

CREATE TABLE meta.entity_attribute (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.idmap;

CREATE TABLE meta.idmap (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.idmap_column;

CREATE TABLE meta.idmap_column (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- Drop table

-- DROP TABLE meta.queue_column;

CREATE TABLE meta.queue_column (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.queue_etl_log;

CREATE TABLE meta.queue_etl_log (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.queue_table;

CREATE TABLE meta.queue_table (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.queue_table_etl;

CREATE TABLE meta.queue_table_etl (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.queue_table_etl_log;

CREATE TABLE meta.queue_table_etl_log (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.queue_table_last_etl_log;

CREATE TABLE meta.queue_table_last_etl_log (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta."source";

CREATE TABLE meta."source" (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.source_column;

CREATE TABLE meta.source_column (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.source_table;

CREATE TABLE meta.source_table (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.status;

-- Drop table

-- DROP TABLE meta.storage_column;

CREATE TABLE meta.storage_column (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.storage_etl_log;

CREATE TABLE meta.storage_etl_log (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta.storage_table;

CREATE TABLE meta.storage_table (
	id uuid NULL,
	value json NULL
);

-- Drop table

-- DROP TABLE meta."transaction";

CREATE TABLE meta."transaction" (
	id uuid NULL,
	value json NULL
);



create table meta.idmap_etl
(
	id uuid, 
	value json
);


CREATE TABLE meta.idmap_last_etl_log (
	id uuid NULL,
	value json NULL
);


CREATE TABLE meta.anchor_table_etl (
	id uuid NULL,
	value json NULL
);


CREATE TABLE meta.anchor_last_etl_log (
	id uuid NULL,
	value json NULL
);

create table meta.table_max_dttm
(
	id uuid null, 
	value json null
);



CREATE TABLE META.QUEUE_TABLE_INCREMENT 
(
	QUEUE_TABLE_ID UUID,
	INCREMENT_DATETIME TIMESTAMP
);

CREATE TABLE meta.stg_table_etl_log 
(
	etl_id BIGINT, 
	job_id uuid, 
	stg_table_id uuid,
	status_id INT,
	error_text TEXT,
	start_datetime timestamp,
	end_datetime timestamp
);

CREATE TABLE meta.idmap_etl_log
(
	etl_id BIGINT,
	job_id uuid,
	idmap_id uuid,
	stg_table_id uuid,
	status_id INT,
	error_text TEXT,
	start_datetime timestamp,
	end_datetime timestamp
);

CREATE TABLE meta.anchor_etl_log
(
	etl_id BIGINT,
	job_id uuid,
	anchor_id uuid,
	status_id INT,
	error_text TEXT,
	start_datetime timestamp,
	end_datetime timestamp
);

CREATE TABLE meta.attribute_etl_log
(
	etl_id BIGINT,
	job_id uuid,
	attribute_id uuid,
	stg_table_id uuid,
	status_id INT,
	error_text TEXT,
	start_datetime timestamp,
	end_datetime timestamp
);

CREATE TABLE meta.tie_etl_log
(
	etl_id BIGINT,
	job_id uuid,
	tie_id uuid,
	stg_table_id uuid,
	status_id INT,
	error_text TEXT,
	start_datetime timestamp,
	end_datetime timestamp
);


CREATE TABLE meta.job_log
(
	job_id uuid,
	status_id INT,
	start_datetime timestamp, 
	end_datetime timestamp
);

CREATE TABLE meta.current_job
(
	job_id uuid,
	status_id INT,
	start_datetime timestamp, 
	end_datetime timestamp
);

CREATE TABLE meta.etl_status 
(
	status_id INT,
	status_desc VARCHAR(100)
);

INSERT INTO meta.etl_status 
(status_id, status_desc)
values 
(1, 'Successful load'),
(2, 'Error'),
(0, 'In process');



