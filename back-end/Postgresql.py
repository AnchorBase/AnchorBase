# coding=utf-8
import psycopg2
import psycopg2.extensions
from Constants import *

#TODO: проблема со вставкой русских символов

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

def sql_exec(
        p_database: str,
        p_server: str,
        p_user: str,
        p_password: str,
        p_port: int,
        p_sql: str,
        p_result: int =1,
        p_rollback: int =0
):
    """
    Выполняет запросы в PostgreSQL
    :param p_database: база данных
    :param p_server: сервер
    :param p_user: пользователь
    :param p_password: пароль
    :param p_port: порт
    :param p_sql: SQL-запрос
    :param p_result: Признак наличия результата запроса (по умолчанию 1)
    :param p_rollback: Признак необходимости отката транзакции в любом случае (по умолчанию 0)
    """
    l_query_output=None
    l_error=None
    try: # проверяем подключение
        cnct = psycopg2.connect(
            dbname=p_database,
            user=p_user,
            password=p_password,
            host=p_server,
            port=p_port
        )
    except psycopg2.OperationalError as e:
        # sys.exit(e) #TODO: реализовать вывод ошибок, как сделал Рустем
        l_error=e
    cnct.autocommit = False
    crsr = cnct.cursor()
    try:
        crsr.execute(p_sql)
        if p_result==1: # если нужен результат запроса
            l_query_output = crsr.fetchall()
        else:
            l_query_output = 1
        if p_rollback==1:
            cnct.rollback() # откат транзакции, если признак - 1
        else:
            cnct.commit() # в остальных случаях - комит транзакции
    except psycopg2.Error as e:
        cnct.rollback() # при возникновении ошибки - откат транзакции
        # sys.exit(e) #TODO: реализовать вывод ошибок, как сделал Рустем
        l_error=e
    finally:
        crsr.close()
        cnct.close()
    return l_query_output, l_error

C_CURRENT_TIMESTAMP_SQL="CURRENT_TIMESTAMP"

def get_source_table_etl(
        p_source_table_id: str,
        p_source_attribute: str,
        p_source_attribute_value: str
):
    """
    Генерирует ETL-скрипт для queue таблицы

    :param p_source_table_id: id таблицы
    :param p_source_attribute: атрибуты таблицы (список должен быть отсортирован в соответствии с наименованием атрибута)
    :param p_source_attribute_value: значения атрибутов
    """
    l_etl="DELETE FROM "+'"'+C_STG_SCHEMA+'"'+"."+'"'+str(p_source_table_id)+'";'+"\n\t" \
           "INSERT INTO "+'"'+C_STG_SCHEMA+'"'+"."+'"'+str(p_source_table_id)+'"'+"\n\t"\
          "("+p_source_attribute+")\n\t"\
          "VALUES\n"+p_source_attribute_value+";\n"
    return l_etl


def get_idmap_etl(
      p_idmap_id: str,
      p_source_table_id: str,
      p_attribute_nk: str,
      p_source_id: str,
      p_idmap_nk_id: str,
      p_idmap_rk_id: str,
      p_etl_id: str,
      p_etl_value: str
):
    """
    Генерирует ETL для Idmap

    :param p_idmap_id: id idmap
    :param p_source_table_id: id таблицы источника
    :param p_attribute_nk: скрипт конкатенации натуральных ключей из таблицы источника
    :param p_source_id: source_system_id источника
    :param p_idmap_nk_id: id атрибута натурального ключа
    :param p_idmap_rk_id: id атрибута суррогатного ключа
    :param p_etl_id: id атрибута etl_id
    :param p_etl_value: значение для атрибута etl_id
    """
    l_etl="DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+"_1"+'"'+";\n"\
          "CREATE TABLE "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+"_1"+'"'+" AS (\n"\
          "SELECT\n\t"\
          "DISTINCT\n\t"\
          "CAST(\n\t\t"+p_attribute_nk+"\n\t\t||'@@'||\n\t\t"+"CAST('"+str(p_source_id)+"' AS VARCHAR(1000))\n\t"\
          "AS VARCHAR (1000)) AS idmap_nk\n\t"\
          "FROM "+'"'+C_STG_SCHEMA+'"'+"."+'"'+str(p_source_table_id)+'"\n'+");\n"\
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+"_2"+'"'+";\n"\
          "CREATE TABLE "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+"_2"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          "nk.idmap_nk\n\t"\
          ",COALESCE(mx_rk.max_rk,0) + ROW_NUMBER() OVER (ORDER BY 1) AS idmap_rk\n\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+"_1"+'"'+" as nk\n\t"\
          "LEFT JOIN "+'"'+C_IDMAP_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+'"'+" as idmp\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND nk.idmap_nk=idmp."+'"'+str(p_idmap_nk_id)+'"'+"\n\t" \
          "CROSS JOIN (\n\t\tSELECT \n\t\tMAX("+'"'+str(p_idmap_rk_id)+'"'+") as max_rk \n\t\tFROM "\
          +'"'+C_IDMAP_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+'"'+"\n\t) as mx_rk\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND idmp."+'"'+str(p_idmap_rk_id)+'"'+" IS NULL\n);\n"\
          "INSERT INTO idmap."+'"'+str(p_idmap_id)+'"'+"\n"\
          "(\n\t"+'"'+str(p_idmap_rk_id)+'"\n\t'+","+'"'+str(p_idmap_nk_id)+'"\n\t'+","+'"'+str(p_etl_id)+'"\n'+")\n\t"\
          "SELECT\n\t"\
          "idmap_rk,\n\t"\
          "idmap_nk,\n\tCAST('"+str(p_etl_value)+"' AS BIGINT)\n\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+"_2"+'"'+"\n;\n"\
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+"_1"+'"'+";\n"\
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+"_2"+'"'+";"
    return l_etl

def get_anchor_etl(
        p_anchor_id: str,
        p_anchor_rk_id: str,
        p_anchor_source_id: str,
        p_anchor_etl_id: str,
        p_idmap_id: str,
        p_idmap_rk_id: str,
        p_idmap_nk_id: str,
        p_etl_value: str
):
    """
    Герерирует etl для anchor таблицы

    :param p_anchor_id: id якорной таблицы
    :param p_anchor_rk_id: id rk атрибута якорной таблицы
    :param p_anchor_source_id: id атрибута source_system_id
    :param p_anchor_etl_id: id атрибута etl_id
    :param p_idmap_id: id idmap
    :param p_idmap_rk_id: id атрибута rk idmap
    :param p_idmap_nk_id: id атрибута nk idmap
    :param p_etl_value: etl процесс
    """
    l_etl="INSERT INTO "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_anchor_id)+'"'+"\n(\n\t"\
          +'"'+str(p_anchor_rk_id)+'"'+",\n\t"+'"'+str(p_anchor_source_id)+'"'+",\n\t"+'"'+str(p_anchor_etl_id)+'"'+"\n)\n\t"\
          "SELECT\n\t" \
          +'"'+str(p_idmap_rk_id)+'"'+"\n\t"\
         ",CAST(REVERSE(SUBSTR(REVERSE("+'"'+str(p_idmap_nk_id)+'"'+"),1,POSITION('@@' IN REVERSE("+'"'+str(p_idmap_nk_id)+'"'+"))-1)) AS INT)\n\t"\
         ",CAST('"+str(p_etl_value)+"' AS BIGINT)\n\t"\
         "FROM "+'"'+C_IDMAP_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+'"'+"\n\t"\
         "CROSS JOIN (\n\t\t"\
         "SELECT MAX("+'"'+str(p_anchor_rk_id)+'"'+") AS max_rk \n\t\t"\
         "FROM "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_anchor_id)+'"'+"\n\t) AS mx_rk\n\t"\
         "WHERE 1=1\n\t\t"\
         "AND " +'"'+str(p_idmap_rk_id)+'"'+">COALESCE(mx_rk.max_rk,0);"
    return l_etl

def get_attribute_etl(
        p_attribute_id: str,
        p_anchor_rk_id: str,
        p_attribute_column_id: str,
        p_from_dttm_id: str,
        p_to_dttm_id: str,
        p_etl_id: str,
        p_idmap_id: str,
        p_idmap_rk_id: str,
        p_idmap_nk_id: str,
        p_attribute_concat_nk: str,
        p_stg_table_id: str,
        p_stg_attribute_id: str,
        p_update_timestamp_id: str,
        p_source_id: str,
        p_etl_value: str,
        p_data_type: str
):
    """
    Генерирует etl для таблицы attribute

    :param p_attribute_id: id таблицы attribute
    :param p_anchor_rk_id: id атрибута rk таблицы attribute
    :param p_attribute_column_id: id атрибута value таблицы attribute
    :param p_from_dttm_id: id атрибута from таблицы attribute
    :param p_to_dttm_id: id атрибута to таблицы attribute
    :param p_etl_id: id атрибута etl таблицы attribute
    :param p_idmap_id: id таблицы idmap
    :param p_idmap_rk_id: id атрибута rk таблицы idmap
    :param p_idmap_nk_id: id атрибута nk таблицы idmap
    :param p_attribute_concat_nk: скрипт конкатенации натуральных ключей
    :param p_stg_table_id: id таблицы источника
    :param p_stg_attribute_id: id атрибута таблицы источника
    :param p_update_timestamp_id: id атрибута update_timestamp
    :param p_source_id: id источника
    :param p_etl_value: id etl процесса
    :param p_data_type: тип данных атрибута value
    """
    l_partition_etl=get_table_partition_etl(
        p_table_id=p_attribute_id,
        p_temp_table_id=str(p_attribute_id)+"_3"
    )
    l_etl="DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+";\n"\
          "CREATE TABLE "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          "idmap."+'"'+str(p_idmap_rk_id)+'"'+" AS idmap_rk\n\t"\
          ",qe."+'"'+str(p_stg_attribute_id)+'"'+" AS attribute_name\n\t"\
          ",qe."+'"'+str(p_update_timestamp_id)+'"'+" AS from_dttm\n\t"\
          ",ROW_NUMBER() OVER (PARTITION BY idmap."+'"'+str(p_idmap_rk_id)+'"'+" ORDER BY qe."+'"'+str(p_update_timestamp_id)+'"'+") AS rnum\n\t"\
          "FROM "+'"'+C_STG_SCHEMA+'"'+"."+'"'+str(p_stg_table_id)+'"'+" AS qe\n\t"\
          "INNER JOIN "+'"'+C_IDMAP_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+'"'+" AS idmap\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND CAST(\n\t\t\t"+p_attribute_concat_nk+"\n\t\t\t||'@@'||\n\t\t\tCAST('"+str(p_source_id)+"' AS VARCHAR(1000)) \n\t\tAS VARCHAR(1000))=idmap."+'"'+str(p_idmap_nk_id)+'"\n);\n'\
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_2"+'"'+";\n" \
          "CREATE TABLE "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_2"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          " crow.idmap_rk\n\t"\
          ",crow.attribute_name\n\t"\
          ",crow.from_dttm\n\t"\
          ",crow.rnum\n\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+" AS crow\n\t"\
          "LEFT JOIN "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+" AS prow\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND crow.idmap_rk=prow.idmap_rk\n\t\t"\
          "AND crow.rnum=prow.rnum+1\n\t\t"\
          "AND crow.attribute_name=prow.attribute_name\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND prow.idmap_rk IS NULL\n);\n"\
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+";\n"\
          "CREATE TABLE "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          "vers.idmap_rk\n\t"\
          ",vers.attribute_name\n\t"\
          ",vers."+'"'+C_FROM_ATTRIBUTE_NAME+'"'+"\n\t"\
          ",COALESCE(\n\t\tLEAD(vers."+'"'+C_FROM_ATTRIBUTE_NAME+'"'+") OVER (PARTITION BY vers.idmap_rk ORDER BY vers.RNUM ASC)-INTERVAL'1'SECOND\n\t\t"\
          ",CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n\t) AS "+'"'+C_TO_ATTRIBUTE_NAME+'"'+"\n\t"\
          ",lv."+'"'+str(p_attribute_column_id)+'"'+" AS prev_attribute_name\n\t"\
          ",lv."+'"'+str(p_from_dttm_id)+'"'+" AS prev_from_dttm\n\t"\
          ",vers."+'"'+C_FROM_ATTRIBUTE_NAME+'"'+"- INTERVAL'1'SECOND AS new_to_dttm\n\t"\
          ",lv."+'"'+str(p_etl_id)+'"'+" as prev_etl_id\n\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_2"+'"'+" AS vers\n\t"\
          "LEFT JOIN "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+'"'+" AS lv\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND vers.idmap_rk=lv."+'"'+str(p_anchor_rk_id)+'"'+"\n\t\t"\
          "AND lv."+'"'+str(p_to_dttm_id)+'"'+"=CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n\t\t"\
          "AND vers.rnum=1\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND COALESCE(CAST(vers.attribute_name AS VARCHAR(1000)),CAST('###' AS CHAR(3)))<>"\
          "COALESCE(CAST(lv."+'"'+str(p_attribute_column_id)+'"'+" AS VARCHAR(1000)),CAST('###' AS CHAR(3)))\n);\n"\
          "DELETE\n\t"\
          "FROM "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+'"'+" AS attr\n\t"\
          "USING "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+" AS tmp\n"\
          "WHERE 1=1\n\t"\
          "AND attr."+'"'+str(p_anchor_rk_id)+'"'+"=tmp.idmap_rk\n\t"\
          "AND attr."+'"'+str(p_to_dttm_id)+'"'+"=CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n;\n"\
          "INSERT INTO "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+'"'+"\n(\n\t"\
           +'"'+str(p_anchor_rk_id)+'"'+",\n\t"+'"'+str(p_attribute_column_id)+'"'+",\n\t"\
           +'"'+str(p_from_dttm_id)+'"'+",\n\t"+'"'+str(p_to_dttm_id)+'"'+",\n\t" \
           +'"'+str(p_etl_id)+'"'+"\n)\n\t"\
          "SELECT\n\t"\
          " idmap_rk\n\t"\
          ",prev_attribute_name\n\t"\
          ",prev_from_dttm\n\t"\
          ",new_to_dttm\n\t"\
          ",prev_etl_id\n\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+"\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND prev_from_dttm IS NOT NULL\n;\n"+l_partition_etl+"\n"\
          "INSERT INTO "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+'"'+"\n(\n\t" \
          +'"'+str(p_anchor_rk_id)+'"'+",\n\t"+'"'+str(p_attribute_column_id)+'"'+",\n\t" \
          +'"'+str(p_from_dttm_id)+'"'+",\n\t"+'"'+str(p_to_dttm_id)+'"'+",\n\t" \
          +'"'+str(p_etl_id)+'"'+"\n)\n\t" \
         "SELECT\n\t"\
         " idmap_rk\n\t,CAST(attribute_name AS "+str(p_data_type)+")\n\t,from_dttm\n\t,to_dttm\n\t,CAST("+str(p_etl_value)+" AS BIGINT)\n\t"\
         "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+"\n;\n" \
         "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+";\n" \
         "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_2"+'"'+";\n" \
         "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+";"

    return l_etl

def get_tie_etl(
        p_tie_id: str,
        p_anchor_rk: str,
        p_link_anchor_rk: str,
        p_from_dttm_id: str,
        p_to_dttm_id: str,
        p_etl_id: str,
        p_idmap_id: str,
        p_idmap_rk_id: str,
        p_idmap_nk_id: str,
        p_idmap_concat: str,
        p_link_idmap_id: str,
        p_link_idmap_rk_id: str,
        p_link_idmap_nk_id: str,
        p_link_idmap_concat: str,
        p_stg_table_id: str,
        p_update_timestamp_id: str,
        p_source_id: str,
        p_etl_value: str
):
    """
    Генерирует etl для таблицы tie

    :param p_tie_id: id таблицы tie
    :param p_anchor_rk: id атрибута rk таблицы tie
    :param p_link_anchor_rk: id атрибута link_rk таблицы tie
    :param p_from_dttm_id: id атрибута from таблицы tie
    :param p_to_dttm_id: id атрибута to таблицы tie
    :param p_etl_id: id атрибута etl таблицы tie
    :param p_idmap_id: id idmap таблицы
    :param p_idmap_rk_id: id атрибута rk таблицы idmap
    :param p_idmap_nk_id: id атрибута nk таблицы idmap
    :param p_idmap_concat: конкатенация натуральных ключей для idmap
    :param p_link_idmap_id: id link_idmap таблицы
    :param p_link_idmap_rk_id: id атрибута rk таблицы link_idmap
    :param p_link_idmap_nk_id: id атрибута nk таблицы link_idmap
    :param p_link_idmap_concat: конкатенация натуральных ключей для link_idmap
    :param p_stg_table_id: id таблицы источника
    :param p_update_timestamp_id: id атрибута update таблицы источника
    :param p_source_id: id источника
    :param p_etl_value: id etl процесса
    """
    l_partition_etl=get_table_partition_etl(
        p_table_id=p_tie_id,
        p_temp_table_id=str(p_tie_id)+"_3"
    )
    l_etl="DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+";\n"\
          "CREATE TABLE "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          "idmap."+'"'+str(p_idmap_rk_id)+'"'+" AS idmap_rk\n\t"\
          ",l_idmap."+'"'+str(p_link_idmap_rk_id)+'"'+" AS link_idmap_rk\n\t"\
          ",qe."+'"'+str(p_update_timestamp_id)+'"'+" AS from_dttm\n\t"\
          ",ROW_NUMBER() OVER (PARTITION BY idmap."+'"'+str(p_idmap_rk_id)+'"'+" ORDER BY qe."+'"'+str(p_update_timestamp_id)+'"'+") AS rnum\n\t"\
          "FROM "+'"'+C_STG_SCHEMA+'"'+"."+'"'+str(p_stg_table_id)+'"'+" AS qe\n\t"\
          "INNER JOIN "+'"'+C_IDMAP_SCHEMA+'"'+"."+'"'+str(p_idmap_id)+'"'+" AS idmap\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND CAST(\n\t\t\t"+p_idmap_concat+"\n\t\t\t||'@@'||\n\t\t\tCAST('"+str(p_source_id)+"' as VARCHAR(1000))\n\t\t as VARCHAR(1000))=idmap."\
          +'"'+str(p_idmap_nk_id)+'"'+"\n\t"\
          "INNER JOIN "+'"'+C_IDMAP_SCHEMA+'"'+"."+'"'+str(p_link_idmap_id)+'"'+" AS l_idmap\n\t\t"\
          "ON 1=1\n\t\t" \
          "AND CAST(\n\t\t\t"+p_link_idmap_concat+"\n\t\t\t||'@@'||\n\t\t\tCAST('"+str(p_source_id)+"' as VARCHAR(1000))\n\t\t as VARCHAR(1000))=l_idmap."\
          +'"'+str(p_link_idmap_nk_id)+'"'+"\n);\n" \
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_2"+'"'+";\n"\
          "CREATE TABLE "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_2"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          " crow.idmap_rk\n\t"\
          ",crow.link_idmap_rk\n\t"\
          ",crow.from_dttm\n\t"\
          ",crow.rnum\n\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+" AS crow\n\t"\
          "LEFT JOIN "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+" AS prow\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND crow.idmap_rk=prow.idmap_rk\n\t\t"\
          "AND crow.rnum=prow.rnum+1\n\t\t"\
          "AND crow.link_idmap_rk=prow.link_idmap_rk\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND prow.idmap_rk IS NULL\n);"\
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+";\n"\
          "CREATE TABLE "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+" AS \n(\n\t"\
          "SELECT\n\t"\
          " vers.idmap_rk as anchor_rk\n\t"\
          ",vers.link_idmap_rk as link_anchor_rk\n\t"\
          ",vers.from_dttm\n\t"\
          ",COALESCE(\n\t\t"\
          "LEAD(vers.from_dttm) OVER (PARTITION BY vers.idmap_rk ORDER BY vers.RNUM ASC)-INTERVAL'1'SECOND\n\t\t"\
          ",CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n\t"\
          ") AS to_dttm\n\t"\
          ",lv."+'"'+str(p_link_anchor_rk)+'"'+" AS prev_link_anchor_rk\n\t"\
          ",lv."+'"'+str(p_from_dttm_id)+'"'+" AS prev_from_dttm\n\t"\
          ",vers.from_dttm - INTERVAL'1'SECOND AS new_to_dttm\n\t"\
          ",lv."+'"'+str(p_etl_id)+'"'+" as prev_etl_id\n\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_2"+'"'+" AS vers\n\t"\
          "LEFT JOIN "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_tie_id)+'"'+" AS lv\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND vers.idmap_rk=lv."+'"'+str(p_anchor_rk)+'"'+"\n\t\t"\
          "AND lv."+'"'+str(p_to_dttm_id)+'"'+"=CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n\t\t"\
          "AND vers.rnum=1\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND vers.link_idmap_rk<>COALESCE(lv."+'"'+str(p_link_anchor_rk)+'"'+",CAST(-1 AS BIGINT))\n);\n"\
          "DELETE\n\t"\
          "FROM "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_tie_id)+'"'+" AS attr\n\t"\
          "USING "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+" AS tmp\n"\
          "WHERE 1=1\n\t"\
          "AND attr."+'"'+str(p_anchor_rk)+'"'+"=tmp.anchor_rk\n\t"\
          "AND attr."+'"'+str(p_to_dttm_id)+'"'+"=CAST('5999-12-31 00:00:00' AS TIMESTAMP);\n"\
          "INSERT INTO "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_tie_id)+'"'+"\n(\n\t" \
          +'"'+str(p_anchor_rk)+'"'+",\n\t"+'"'+str(p_link_anchor_rk)+'"'+",\n\t" \
          +'"'+str(p_from_dttm_id)+'"'+",\n\t"+'"'+str(p_to_dttm_id)+'"'+",\n\t" \
          +'"'+str(p_etl_id)+'"'+"\n)\n\t"\
          "SELECT\n\t"\
          " anchor_rk\n\t"\
          ",prev_link_anchor_rk\n\t"\
          ",prev_from_dttm\n\t"\
          ",new_to_dttm\n\t"\
          ",prev_etl_id\n\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+"\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND prev_from_dttm IS NOT NULL\n;\n"+l_partition_etl+"\n"\
          "INSERT INTO "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_tie_id)+'"'+"\n(\n\t" \
          +'"'+str(p_anchor_rk)+'"'+",\n\t"+'"'+str(p_link_anchor_rk)+'"'+",\n\t" \
          +'"'+str(p_from_dttm_id)+'"'+",\n\t"+'"'+str(p_to_dttm_id)+'"'+",\n\t" \
          +'"'+str(p_etl_id)+'"'+"\n)\n\t" \
          "SELECT\n\t" \
          " anchor_rk\n\t" \
          ",link_anchor_rk\n\t" \
          ",from_dttm\n\t" \
          ",to_dttm\n\t" \
          ",CAST('"+str(p_etl_value)+"' AS BIGINT)\n\t" \
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+"\n;\n" \
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+";\n" \
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_2"+'"'+";\n" \
          "DROP TABLE IF EXISTS "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+";"
    return l_etl

def get_table_partition_etl(
        p_table_id: str,
        p_temp_table_id: str
):
    """
    Генерирует скрипт для генерации партиций

    :param p_table_id: id целевой таблицы
    :param p_temp_table_id: id временной таблицы
    """
    l_etl="DO $$\n"\
          "DECLARE\n\t"\
          "v_n_prt_date DATE;\n"\
          "BEGIN\n\t"\
          "FOR v_n_prt_date IN\n\t\t"\
          "SELECT\n\t\t"\
          "DISTINCT\n\t\t"\
          "CAST(DATE_TRUNC('month', tmp."+'"'+C_FROM_ATTRIBUTE_NAME+'"'+") + "\
          "INTERVAL '1 month - 1 day' AS DATE) AS new_partition_date\n\t\t"\
          "FROM "+'"'+C_WRK_SCHEMA+'"'+"."+'"'+str(p_temp_table_id)+'"'+" tmp\n\t\t"\
          "LEFT JOIN\n\t\t(\n\t\t\t"\
          "SELECT\n\t\t\t"\
          "TO_DATE(SUBSTR(chld.relname,POSITION('_date' IN chld.relname)+5,10),'YYYY-MM-DD') AS partition_date\n\t\t\t"\
          "FROM pg_catalog.pg_class par\n\t\t\t"\
          "LEFT JOIN pg_catalog.pg_inherits inh\n\t\t\t\t"\
          "ON 1=1\n\t\t\t\t"\
          "AND inh.inhparent=par.oid\n\t\t\t"\
          "LEFT JOIN pg_catalog.pg_class chld\n\t\t\t\t"\
          "ON 1=1\n\t\t\t\t"\
          "AND inh.inhrelid=chld.oid\n\t\t\t"\
          "WHERE 1=1\n\t\t\t\t"\
          "AND par.relname='"+str(p_table_id)+"'\n\t\t"\
          ") AS prt\n\t\t\t"\
          "ON 1=1\n\t\t\t"\
          "AND CAST(DATE_TRUNC('month', CAST(tmp."+'"'+C_FROM_ATTRIBUTE_NAME+'"'+" AS DATE)) "\
          "+ INTERVAL '1 month - 1 day' AS DATE)=prt.partition_date\n\t\t"\
          "WHERE 1=1\n\t\t\t"\
          "AND prt.partition_date IS NULL\n\t"\
          "LOOP\n\t\t"\
          "EXECUTE 'CREATE TABLE "+'"'+str(p_table_id)+"'||v_n_prt_date||"+"'"+'"'+"'"+"||\n\t\t\t"\
          "'PARTITION OF "+'"'+C_AM_SCHEMA+'"'+"."+'"'+str(p_table_id)+'"'+" FOR VALUES '||\n\t\t\t"\
          "'FROM ('''||CAST(DATE_TRUNC('month',CAST(v_n_prt_date AS DATE)) AS DATE)||' 00:00:00'') TO ('''||v_n_prt_date||' 23:59:59'');';\n\t"\
          "END LOOP;\n"\
          "END;\n"\
          "$$\n"\
          "LANGUAGE 'plpgsql';"

    return l_etl

def get_entity_function_sql(
    p_entity_name: str,
    p_entity_attribute_dict: dict
):
    """
    Генерация скрипта создания функции-конструктора запросов для сущности

    :param p_entity_name: наименование сущности
    :param p_entity_attribute_dict: словарь с атрибутами сущности и их типами данных
    """
    l_entity_attribute_name_list=list(p_entity_attribute_dict.keys()) # лист с наименованиями атрибутов сущности
    l_entity_attribute_name_param="" # атрибуты сущности через запятую
    l_entity_attribute_name_datatype="" # атрибуты сущности через запятую с типами данных
    l_entity_attribute_var="" # список переменных функции для атрибутов сущности
    l_entity_attribute_join_sql="" # список переменных со скриптами джоинов атрибутов сущности
    l_entity_attribute_select_sql="" # скрипт переменной для формирования select
    l_entity_attribute_sql="" # скрипт переменной финального sql
    for i_entity_attribute in l_entity_attribute_name_list:
        l_entity_attribute_name_param+=str(i_entity_attribute)+","
        l_entity_attribute_name_datatype+='"'+str(i_entity_attribute)+'"'+" "+p_entity_attribute_dict.get(i_entity_attribute)+",\n\t"
        l_entity_attribute_var+="v_"+str(i_entity_attribute)+" char(1):=(select case when v_col like '%,"+str(i_entity_attribute)+\
                                ",%' then '' else NULL end);\n\t"
        l_entity_attribute_join_sql+="v_"+str(i_entity_attribute)+"_join_sql varchar(1000):=("\
                                     "case when v_"+str(i_entity_attribute)+" is not null "\
                                     "then ' left join "+C_AM_SCHEMA+"."+'"'+p_entity_name+"_"+str(i_entity_attribute)+C_TABLE_NAME_POSTFIX.get(C_ATTRIBUTE)+'"'+""\
                                     " as "+'"'+str(i_entity_attribute)+'"'+" on "+'"main"'+"."+p_entity_name+"_"+C_RK+"="+""\
                                     '"'+str(i_entity_attribute)+'"'+"."+p_entity_name+"_"+C_RK+""\
                                     " and cast('''||dt||''' as timestamp) between "+'"'+str(i_entity_attribute)+'"'+"."+C_FROM_ATTRIBUTE_NAME+" and "+'"'+str(i_entity_attribute)+'"'+"."+C_TO_ATTRIBUTE_NAME+"'"\
                                     " else '' end);\n\t"
        l_entity_attribute_select_sql+="coalesce(v_"+str(i_entity_attribute)+"||'"+str(i_entity_attribute)+",','NULL::"+p_entity_attribute_dict.get(i_entity_attribute)+",')||"
        l_entity_attribute_sql+="coalesce(v_"+str(i_entity_attribute)+"_join_sql,'')||"
    # удаляем лишние символы (последние запятые и тд)
    l_entity_attribute_name_param=l_entity_attribute_name_param[:-1]
    l_entity_attribute_select_sql=l_entity_attribute_select_sql[:-2]
    l_sql="DROP FUNCTION IF EXISTS "+p_entity_name+"(TIMESTAMP, VARCHAR(1000));\n" \
          "CREATE OR REPLACE FUNCTION "+'"'+p_entity_name+'"'+"(\n\t"\
          "dt TIMESTAMP DEFAULT '5999-12-31'::TIMESTAMP,\n\t"\
          "col VARCHAR(1000) DEFAULT '"+l_entity_attribute_name_param+"'\n) RETURNS\n"\
          "TABLE(\n\t"+p_entity_name+"_"+C_RK+" "+C_BIGINT+",\n\t"+l_entity_attribute_name_datatype+C_SOURCE_ATTRIBUTE_NAME+" "+C_INT+"\n) AS\n"\
          "$$\ndeclare\n\tv_col varchar(1000) :=','||replace(col,' ','')||',';\n\t"+l_entity_attribute_var+""\
          "v_from_sql varchar(1000):=(' from "+C_AM_SCHEMA+"."+'"'+p_entity_name+C_TABLE_NAME_POSTFIX.get(C_ANCHOR)+'"'+" as "+'"main"'+"');\n\t"+l_entity_attribute_join_sql+""\
          "v_select_sql varchar(1000) := ' select main."+p_entity_name+"_"+C_RK+",'||regexp_replace("+l_entity_attribute_select_sql+",',$','')"+""\
          "||',main."+C_SOURCE_ATTRIBUTE_NAME+"';\n\t"\
          "v_sql varchar(1000):=v_select_sql||' '||v_from_sql||' '||"+l_entity_attribute_sql+"';'"+";\n"\
          "begin\n\treturn query execute v_sql;\nend;\n$$\nlanguage plpgsql;"
    return l_sql