
delete from meta.dwh;
delete from meta.transaction;
delete from meta.queue_table_etl;
delete from meta.anchor_table_etl;
delete from meta.anchor_last_etl_log;
delete from meta.queue_table_etl_log;
delete from meta.queue_table_last_etl_log;
delete from meta.queue_table;
delete from meta.source;
delete from meta.source_table;
delete from meta.source_column;
delete from meta.entity;
delete from meta.entity_attribute;
delete from meta.anchor_table;
delete from meta.anchor_column;
delete from meta.queue_column;
delete from meta.storage_table;
delete from meta.storage_column;
delete from meta.idmap_etl;
delete from meta.table_max_dttm;
delete from meta.idmap;
delete from meta.idmap_column;
delete from meta.status;
delete from meta.queue_etl_log;
delete from meta.storage_etl_log;
delete from meta.idmap_etl_log;
delete from meta.anchor_etl_log;
delete from meta.idmap_last_etl_log;

drop schema stg cascade;
create schema stg;
drop schema anch cascade;
create schema anch;
drop schema idmap cascade;
create schema idmap;

