import pyodbc
from platform import system
from Constants import *

def sql_exec(
        p_server: str,
        p_database: str,
        p_user: str,
        p_password: str,
        p_port: int,
        p_sql: str,
        p_result: int =1
):
    """
    Выполняет запросы на MSSQL
    :param p_server: сервер
    :param p_database: БД
    :param p_user: пользователь
    :param p_password: пароль
    :param p_port: порт
    :param p_result: признак наличия результата запроса (по умолчанию 1)
    """
    l_error=None
    query_output=None
    try:
        if system() == "Darwin": # если macos нужно прописать путь до драйвера
            mssql_cnct = pyodbc.connect(
                server=p_server,
                database=p_database,
                uid=p_user,
                tds_version=C_TDS_VERSION,
                pwd=p_password,
                port=p_port,
                driver=C_MSSQL_DRIVER_MACOS_PATH,
                timeout=10
            )
        else:
            mssql_cnct = pyodbc.connect(
                server=p_server,
                database=p_database,
                uid=p_user,
                pwd=p_password,
                port=p_port,
                timeout=10
            )
    except pyodbc.Error as e:
        l_error=e
    crsr =mssql_cnct.cursor()
    try:
        crsr.execute(p_sql)
        if p_result==1:
            query_output=crsr.fetchall()
        else:
            query_output=1
    except pyodbc.Error as e:
        l_error=e
    finally:
        crsr.close()
        mssql_cnct.close()

    return query_output, l_error

def get_objects(
        p_server: str,
        p_database: str,
        p_user: str,
        p_password: str,
        p_port: int
):
    l_sql="""
SELECT
    tab.table_catalog,
    tab.table_schema,
    tab.table_name,
    tab.table_type,
    col.column_name,
    col.data_type,
    COALESCE(
        col.character_maximum_length,
        CASE WHEN col.numeric_scale<>0 AND col.data_type='decimal'
    THEN col.numeric_precision
    END
    ) AS character_maximum_length,
    col.numeric_precision,
    col.numeric_scale,
    CASE WHEN ky.constraint_name IS NOT NULL THEN 1 ELSE 0 END
    FROM information_schema.tables tab
    LEFT JOIN information_schema.columns col
        ON 1=1
        AND tab.table_catalog=col.table_catalog
        AND tab.table_schema=col.table_schema
        AND tab.table_name=col.table_name
    LEFT JOIN information_schema.key_column_usage ky  
        ON 1=1  
        AND tab.table_catalog=ky.table_catalog  
        AND tab.table_schema=ky.table_schema  
        AND tab.table_name=ky.table_name  
        AND col.column_name=ky.column_name  
        AND substring(ky.constraint_name,1,2)='PK'
"""
    l_result=sql_exec(
        p_server=p_server,
        p_database=p_database,
        p_user=p_user,
        p_password=p_password,
        p_port=p_port,
        p_sql=l_sql,
        p_result=1
    )
    return l_result

C_CURRENT_TIMESTAMP_SQL="CURRENT_TIMESTAMP"

