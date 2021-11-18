import pyodbc
import Support
import sys
import struct
import datetime
import json
from platform import system
# если запускать программу на macos обязательно нужно использовать freetds и файл libtdsobc
class Connection:


    # проверяет подключение к MSSQL
    @staticmethod
    def connect_check(cnct_attr):
        server=cnct_attr.get("server",None)
        database=cnct_attr.get("database",None)
        user=cnct_attr.get("user",None)
        password=cnct_attr.get("password",None)
        port=cnct_attr.get("port",None)
        try:
            pyodbc.connect(
                server=server,
                database=database,
                uid=user,
                tds_version='7.3',
                pwd=password,
                port=port,
                driver='/usr/local/lib/libtdsodbc.so',
                timeout=10
            )
        except:
            return False
        else: return True

    # подключается к MSSQL и выполняет запросы
    @staticmethod
    def sql_exec(cnct_attr, sql):
        server=cnct_attr.get("server",None)
        database=cnct_attr.get("database",None)
        user=cnct_attr.get("user",None)
        password=cnct_attr.get("password",None)
        port=cnct_attr.get("port",None)
        try:
            if system() == "Windows":
                l_driver = cnct_attr.get("driver", None)
                mssql_cnct = pyodbc.connect(
                    server=server,
                    database=database,
                    uid=user,
                    tds_version='7.3',
                    pwd=password,
                    port=port,
                    driver=l_driver,
                    timeout=10
                )
            else:
                mssql_cnct = pyodbc.connect(
                    server=server,
                    database=database,
                    uid=user,
                    tds_version='7.3',
                    pwd=password,
                    port=port,
                    driver='/usr/local/lib/libtdsodbc.so',
                    timeout=10
                )
            crsr =mssql_cnct.cursor()
            crsr.execute(sql)
            query_output=crsr.fetchall()
        except pyodbc.Error as e:
            error = Support.Support.error_output("MSSQL","sql_exec",e)
            sys.exit(error)
        else: return query_output

class SQLScript:

    @staticmethod
    def select_object_sql(database=None, schema=None, table=None):
        select = "select " \
                 "tab.table_catalog, " \
                 "tab.table_schema, " \
                 "tab.table_name, " \
                 "tab.table_type, " \
                 "col.column_name, " \
                 "col.data_type, " \
                 "coalesce(col.character_maximum_length,case when col.numeric_scale<>0 and col.data_type='decimal' then col.numeric_precision end) as character_maximum_length, " \
                 "col.numeric_precision, " \
                 "col.numeric_scale, " \
                 "case when ky.constraint_name is not null then 1 else 0 end " \
                 "from information_schema.tables tab " \
                 "left join information_schema.columns col " \
                 "on 1=1 " \
                 "and tab.table_catalog=col.table_catalog " \
                 "and tab.table_schema=col.table_schema " \
                 "and tab.table_name=col.table_name " \
                 "left join information_schema.key_column_usage ky " \
                 "on 1=1 " \
                 "and tab.table_catalog=ky.table_catalog " \
                 "and tab.table_schema=ky.table_schema " \
                 "and tab.table_name=ky.table_name " \
                 "and col.column_name=ky.column_name " \
                 "and substring(ky.constraint_name,1,2)='PK'"
        fltr = " where 1=1"
        if database is not None:
            fltr = fltr + " and tab.table_catalog='"+database+"'"
        if schema is not None:
            fltr = fltr + " and tab.table_schema='"+schema+"'"
        if table is not None:
            fltr = fltr + " and tab.table_name='"+table+"'"
        scrpt = select + fltr + ";"
        return scrpt

    @staticmethod
    def top_raw(schema,table):
        scrpt="select top 10 * from "+str(schema)+"."+str(table)
        return scrpt

    # скрипт отбирающий данные из таблицы по инкременту (если последний указан)
    @staticmethod
    def select_data_sql(schema, table, column, increment=None, increment_datatype=None):
        clmn_script=""
        for a in column:
            clmn_script=clmn_script+'"'+str(a)+'"'+","
        clmn_script=clmn_script[:-1]
        incr_script=""
        if increment is not None and increment_datatype is not None:
            incr_script=" where "+'"'+str(increment)+'"'+">cast(@ as "+str(increment_datatype)+")"
        scrpt="select "+clmn_script+" from "+'"'+str(schema)+'"'+"."+'"'+str(table)+'"'+" with (nolock)"+incr_script+";"
        return scrpt
    # подстановка дефолтных значений инкремента
    @staticmethod
    def increment_default_value(increment_datatype):
        if increment_datatype in {"int","bigint","smallint","timestamp","rowversion"}:
            return 0
        elif increment_datatype in {"date","datetime"}:
            return "1900-01-01"
