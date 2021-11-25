import pyodbc
import Support
import sys
import datetime
import json
import MSSQL
import Postgresql
import Metadata
from pathlib import Path
import FileWorker
from SystemObjects import Constant as const


C_DWH_TYPE=Metadata.Metadata.select_meta("dwh",None,0)[0].get("type",None).lower() # тип DWH
dwh_cnct_attr=Metadata.Metadata.select_meta("dwh",None,0)[0]

class Driver:
    #TODO: устаревший/переделать

    available_source=["mssql"]
    available_dwh=["postgresql"]
    dwh_type=Metadata.Metadata.select_meta("dwh",None,0)[0].get("type",None).lower()
    dwh_cnct_attr=Metadata.Metadata.select_meta("dwh",None,0)[0]

    @staticmethod
    def cnct_type_check(type):
        if type not in Driver.available_source and type not in Driver.available_dwh:
            error = Support.Support.error_output("Driver","cnct_type_check","There is no driver for '"+str(type)+"'")
            sys.exit(error)

    @staticmethod
    def sql_exec(cnct_attr,sql, noresult=None):
        type=cnct_attr.get("type",None).lower()
        Driver.cnct_type_check(type)
        if type=="mssql":
            return MSSQL.Connection.sql_exec(cnct_attr, sql)
        if type=="postgresql":
            return Postgresql.Postgres.sql_exec(cnct_attr, sql, noresult)
        else:
            error = Support.Support.error_output("Driver","sql_exec","DBMS '"+str(type)+"' is not allowed")
            sys.exit(error)

    @staticmethod
    def cnct_connect_check(cnct_attr):
        type=cnct_attr.get("type",None).lower()
        if type=="mssql":
            cnct = MSSQL.Connection.connect_check(cnct_attr)
            if cnct==False:
                error = Support.Support.error_output("Source","source_check","Connection failed")
                sys.exit(error)
    @staticmethod
    def select_db_object(cnct_attr,object=None):
        db_object=""
        if object is None:
            object={}
        type=cnct_attr.get("type",None).lower()
        database=object.get("database",None)
        schema=object.get("schema",None)
        table=object.get("table",None)
        if type=="mssql":
            sql=MSSQL.SQLScript.select_object_sql(database,schema,table)
            db_object=MSSQL.Connection.sql_exec(cnct_attr,sql)
        else:
            error = Support.Support.error_output("Driver","select_db_object","Source Type '"+str(type)+"' is not allowed")
            sys.exit(error)
        return db_object

    @staticmethod
    def select_top_raw(cnct_attr, object):
        top_raw=""
        type=cnct_attr.get("type",None).lower()
        database=object.get("database",None)
        schema=object.get("schema",None)
        table=object.get("table",None)
        if type=="mssql":
            sql=MSSQL.SQLScript.top_raw(schema,table)
            top_raw=MSSQL.Connection.sql_exec(cnct_attr,sql)
        else:
            error = Support.Support.error_output("Driver","select_top_raw","Source Type '"+str(type)+"' is not allowed")
            sys.exit(error)
        return top_raw

    # сопоставляет типы данных разных СУБД
    @staticmethod
    def data_type_map(from_datatype, from_dbms, from_length):
        from_datatype=from_datatype.lower()
        from_dbms=from_dbms.lower()
        if from_length!=-1:
            from_length=None
        to_dbms=Driver.dwh_type
        datatype_map=[
            {"from_datatype":"blob","to_datatype":"varbinary","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"binary","to_datatype":"bytea","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"bit","to_datatype":"boolean","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"datetime","to_datatype":"timestamp","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"decimal","to_datatype":"decimal","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"image","to_datatype":"bytea","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"int","to_datatype":"integer","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"nchar","to_datatype":"char","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"ntext","to_datatype":"text","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"nvarchar","to_datatype":"varchar","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"rowversion","to_datatype":"bytea","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"smalldatetime","to_datatype":"timestamp","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":0},
            {"from_datatype":"smallmoney","to_datatype":"money","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"timestamp","to_datatype":"bytea","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"tinyint","to_datatype":"smallint","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"uniqueidentifier","to_datatype":"char","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":36},
            {"from_datatype":"nvarchar","to_datatype":"text","from_dbms":"mssql","to_dbms":"postgresql","from_length":-1,"to_length":None},
            {"from_datatype":"nvarchar","to_datatype":"varchar","from_dbms":"mssql","to_dbms":"postgresql","from_length":None,"to_length":None},
            {"from_datatype":"varchar","to_datatype":"text","from_dbms":"mssql","to_dbms":"postgresql","from_length":-1,"to_length":None}
        ]
        for i in datatype_map:
            if i.get("from_datatype",None)==from_datatype and i.get("from_dbms",None)==from_dbms and i.get("from_length",None)==from_length and i.get("to_dbms")==to_dbms:
                return i
    # выставляет корректный тип данных timestamp для использующейся в dwh СУБД
    @staticmethod
    def convert_to_timestamp():
        if Driver.dwh_type=="postgresql":
            return "timestamp"
        if Driver.dwh_type=="mssql":
            return "datetime"

    @staticmethod
    def convert_to_guid():
        if Driver.dwh_type=="postgresql":
            return "uuid"
        if Driver.dwh_type=="mssql":
            return "uniqueidentifier"

    # генерирует ddl таблицы
    @staticmethod
    def create_table_ddl(schema, table, attr):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.create_table_ddl(schema, table, attr)

    # генерирует ddl view
    @staticmethod
    def create_view_sql(schema, view, source_schema, source_table, attr, vers=None):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.create_view_sql(schema, view, source_schema, source_table, attr, vers)

    # генерирует drop table
    @staticmethod
    def drop_table_sql(schema,table):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.drop_table_sql(schema, table)

    # скрипт, определяющий макс update_dttm
    @staticmethod
    def max_update_dttm_sql(table):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.max_update_dttm_sql(table)

    # скрипт, достающий данный из таблицы источника
    @staticmethod
    def select_data_sql(schema, table, column, increment, increment_datatype,cnct_attr):
        type=cnct_attr.get("type",None).lower()
        if type=="mssql":
            return MSSQL.SQLScript.select_data_sql(schema,table,column,increment,increment_datatype)

    # скрипт создания таблицы шины и вставки в нее данных
    @staticmethod
    def insert_data_into_bus(table, column):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.insert_data_into_bus(table, column)

    # скрипт вставки данных через values
    @staticmethod
    def insert_data_into_queue(table, column, source_table):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.insert_data_into_queue(table, column, source_table)

    # проверка, что у атрибута инкремента соответствующий тип данных
    @staticmethod
    def increment_datatype_check(cnct_attr, datatype):
        type=cnct_attr.get("type",None).lower()
        if type=="mssql":
            if datatype.lower() not in {"date","datetime","timestamp","rowversion","int","bigint","smallint"}:
                error = Support.Support.error_output("Driver","increment_datatype_check","Datatype of increment'"+str(datatype)+"' is not allowed")
                sys.exit(error)
    # дефолтное значение инкремента
    @staticmethod
    def increment_default_value(cnct_attr, datatype):
        type=cnct_attr.get("type",None).lower()
        if type=="mssql":
            return MSSQL.SQLScript.increment_default_value(datatype)

    # скрипт генерации текущих даты и времени
    @staticmethod
    def current_timestamp_sql():
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.current_timestamp_sql()

    # скрипт генерации uuid
    @staticmethod
    def generate_uuid_sql():
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.generate_uuid_sql()

    # генерация суррогатов с помощью idmap
    @staticmethod
    def insert_data_into_idmap(idmap, nkey, queue_table, source_id):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.insert_data_into_idmap(idmap, nkey, queue_table, source_id)

    # запись данных в anchor
    @staticmethod
    def insert_data_into_anchor(anchor_table, idmap_table):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.insert_data_into_anchor(anchor_table, idmap_table)

    # генерация скрипта для вью на актуальный срез
    # работает только для таблиц типа attribute
    @staticmethod
    def create_lv_view_sql(schema, view, lv_view, mxdttm_table, view_rk):
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.create_lv_view_sql(schema, view, lv_view, mxdttm_table, view_rk)


    @staticmethod
    def insert_data_into_attribute(
            attribute_table:str,
            attribute_lv_view:str,
            attribute_column:str,
            anchor_column:str,
            idmap_table:str,
            idmap_key:str,
            mxdttm_table:str,
            queue_obj:list
     ):
        """
        Возвращает SQL-скрипт загрузки данных в таблицу типа attribute для определенной СУБД
        input:
            attribute_table - наименование таблицы типа attribute
            attribute_lv_view - наименование view на актуальный срез таблицы типа attribute
            attribute_column - наименование атрибута в таблице типа attribute
            anchor_column - наименование rk в таблице типа attribute
            idmap_table - наименование idmap сущности
            idmal_key - наименование атрибута nk в idmap
            mxdttm_table - наименование таблицы с максимальными update_dttm для атрибута
            queue_obj - лист с source_id, queue_table, queue_column и queue_key (список из наименований натуральный ключей) [{"source_id":"","queue_table":"","queue_column":"","queue_key":[]}]
        output:
            SQL-скрипт
    """
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.insert_data_into_attribute(
                attribute_table,
                attribute_lv_view,
                attribute_column,
                anchor_column,
                idmap_table,
                idmap_key,
                mxdttm_table,
                queue_obj
            )
    @staticmethod
    def insert_data_into_tie(
            tie_table:str,
            tie_rk1:str,
            tie_rk2:str,
            idmap1_table:str,
            idmap2_table:str,
            idmap_key1:str,
            idmap_key2:str,
            queue_obj:list
    ):
        """
        Возвращает SQL-скрипт загрузки данных в таблицу типа tie для определенной СУБД
        input:
            tie_table - наименование таблицы типа tie
            tie_rk1 - наименование первого ключа tie,
            tie_rk2 - наименование второго ключа tie,
            idmap1_table - наименование idmap для первого ключа,
            idmap2_table - наименование idmap для второго ключа,
            idmap_key1 - наименование натурального ключа idmap для первого ключа,
            idmap_key2 - наименование натурального ключа idmap для второго ключа,
            queue_obj - лист с параметрами источника для ключей tie: source_id, queue_table, queue_key1 и queue_key2 (списки из наименований натуральных ключей) [{"source_id":"","queue_table":"","queue_key1":[], queue_key2":[]}]
        output:
            SQL-скрипт
        """
        if Driver.dwh_type=="postgresql":
            return Postgresql.Postgres.insert_data_into_tie(
                tie_table,
                tie_rk1,
                tie_rk2,
                idmap1_table,
                idmap2_table,
                idmap_key1,
                idmap_key2,
                queue_obj
            )


class DBMS:
    """
    Класс по работе СУБД
    """

    def __init__(self, p_dbms_purpose: str =const('C_DWH').constant_value, p_dbms_type: str = C_DWH_TYPE, p_dbms_cnct_attr: dict =None):
        """
        Конструктор

        :param p_dbms_purpose: назначение СУБД (по дефолту dwh)
        :param p_dbms_type: тип СУБД (по дефолту берется из meta.dwh -> type)
        :param p_dbms_cnct_attr: параметры подключения
        """
        self._dbms_purpose=p_dbms_purpose
        self._dbms_type=p_dbms_type
        self._dbms_cnct_attr=p_dbms_cnct_attr

    @property
    def dbms_purpose(self) -> str:
        """
        Назначение СУБД. Приводит к нижнему регистру и проверяет на корректность значения
        """
        if self._dbms_purpose not in const('C_DBMS_PURPOSE_LIST').constant_value:
            sys.exit("Некорректное назначение СУБД") #TODO: реализовать вывод ошибок, как сделал Рустем
        else:
            return self._dbms_purpose.lower()

    @property
    def dbms_type(self) -> str:
        """
        Тип СУБД. Проверяет, умеет ли AnchorBase работать с таким СУБД. Приводит к нижнему регистру
        """
        if self.dbms_purpose==const('C_SOURCE').constant_value:
            if self._dbms_type not in const('C_AVAILABLE_SOURCE_LIST').constant_value:
                sys.exit("AnchorBase не умеет работать с указанным СУБД") #TODO: реализовать вывод ошибок, как сделал Рустем
        if self.dbms_purpose==const('C_DWH').constant_value:
            if self._dbms_type not in const('C_AVAILABLE_DWH_LIST').constant_value:
                sys.exit("AnchorBase не умеет работать с указанным СУБД") #TODO: реализовать вывод ошибок, как сделал Рустем

        return self._dbms_type.lower()

    @property
    def dbms_cnct_attr(self):
        """
        Параметры подключения к СУБД. Проверяет ключи словаря на полноту указанных параметров
        """
        # если пустой, ничего не делаем
        if self._dbms_cnct_attr is None:
            return self._dbms_cnct_attr

        l_cnct_params = const('C_CNCT_PARARMS').constant_value # фиксированный список параметров подключения
        l_cnct_attr_keys_list = list(self._dbms_cnct_attr.keys()) # определяем ключи словаря с параметрами подключения
        # проверяем, что все ключи указаны корректно
        for i_cnct_attr_keys_list in l_cnct_attr_keys_list:
            if i_cnct_attr_keys_list not in l_cnct_params:
                sys.exit("Некорректно указан параметр подключения") #TODO: реализовать вывод ошибок, как сделал Рустем
        # проверка, что все необходимые параметры указаны
        for i_cnct_params in l_cnct_params:
            if i_cnct_params not in l_cnct_attr_keys_list:
                sys.exit("Не указан параметр подключения"+i_cnct_params) #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._dbms_cnct_attr


class DataType(DBMS):
    """
    Класс типа данных СУБД
    """
    def __init__(self,
                 p_data_type_name: str,
                 p_data_type_length: int =None,
                 p_data_type_scale: int =None,
                 p_dbms_purpose: str =const('C_DWH').constant_value,
                 p_dbms_type: str =C_DWH_TYPE
    ):
        """
        Конструктор

        :param p_dbms_purpose: назначение СУБД (по дефолту dwh)
        :param p_dbms_type: тип СУБД (по дефолту берется из meta.dwh -> type)
        :param p_data_type_name: наименование типа данных
        :param p_data_type_length: длина
        :param p_data_type_scale: количество знаков после запятой
        """
        super().__init__(p_dbms_purpose=p_dbms_purpose, p_dbms_type=p_dbms_type)
        self._data_type_name=p_data_type_name
        self._data_type_length=p_data_type_length
        self._data_type_scale=p_data_type_scale

    @property
    def data_type_name(self) -> str:
        """
        Наименование типа данных.
        Проверка, есть ли указанный тип данных в СУБД/Умеет ли AnchorBase с ним работать.
        Приводит к верхнему регистру
        """
        self._data_type_name=self._data_type_name.lower()
        l_datatype_list=const('C_DBMS_COMPONENTS').constant_value.get(self.dbms_type,None).get(const('C_DATATYPE').constant_value,None)
        if self._data_type_name not in l_datatype_list:
            sys.exit("Некорректно указан тип данных") #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._data_type_name.upper()

    @property
    def data_type_length(self):
        """
        Длина типа данных. Проверка на целое число
        """
        # если пусто, ничего не делаем
        if self._data_type_length is None:
            return self._data_type_length
        elif type(self._data_type_length) is not int:
            sys.exit("Некорректно указана длина типа данных") #TODO: реализовать вывод ошибок, как сделал Рустем
        else:
            return self._data_type_length

    @property
    def data_type_scale(self):
        """
        Количество цифр после запятой. Проверка на целое число
        """
        # если пусто, ничего не делаем
        if self._data_type_scale is None:
            return self._data_type_scale
        elif type(self._data_type_scale) is not int:
            sys.exit("Некорректно указано количество знаков после запятой") #TODO: реализовать вывод ошибок, как сделал Рустем
        else:
            return self._data_type_scale
    @property
    def data_type_sql(self):
        """
        SQL-выражение типа данных
        """
        l_data_type_ddl=self.data_type_name
        if self.data_type_length is not None:
            l_data_type_ddl=l_data_type_ddl+"("+str(self.data_type_length)
            if self.data_type_scale is not None:
                l_data_type_ddl=l_data_type_ddl+","+str(self.data_type_scale)
            l_data_type_ddl=l_data_type_ddl+")"
        return l_data_type_ddl

class Template(DBMS):
    """
    Класс DDL и ETL шаблонов
    """
    C_DDL_TEMPLATE_PATH = Path(const('C_DWH').constant_value.upper(),const('C_DDL_TEMPLATE').constant_value)
    C_ETL_TEMPLATE_PATH = Path(const('C_DWH').constant_value.upper(),const('C_ETL_TEMPLATE').constant_value)
    C_VIEW_TEMPLATE_PATH = Path(const('C_DWH').constant_value.upper(),const('C_VIEW_TEMPLATE').constant_value)
    def __init__(self,
                 p_template_type: str,
                 p_table_type: str,
                 p_dbms_purpose: str =const('C_DWH').constant_value,
                 p_dbms_type: str = C_DWH_TYPE
    ):
        """
        Конструктор

        :param p_template_type: тип шаблона
        :param p_dbms_purpose: назначение СУБД (по умолчанию dwh)
        :param p_dbms_type: тип СУБД (по умолчанию - meta.dwh -> type)
        """
        super().__init__(p_dbms_purpose=p_dbms_purpose,p_dbms_type=p_dbms_type)
        self._template_type=p_template_type
        self._table_type=p_table_type

    @property
    def template_type(self):
        """
        Тип шаблона
        """
        if self._template_type not in const('C_TEMPLATE_TYPE_LIST').constant_value:
            sys.exit("Некорректно указан тип шаблона") #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._template_type.lower()

    @property
    def table_type(self):
        """
        Тип таблицы
        """
        return self._table_type.upper()
    @property
    def template_name(self):
        return self.table_type+"_"+const('C_TEMPLATE_POSTFIX').constant_value+const('C_TEMPLATE_FILE_TYPE').constant_value

    @property
    def template_path(self) -> str:
        """
        Путь к указанному шаблону
        """
        l_template_path=''
        if self.template_type==const('C_DDL').constant_value:
            l_template_path=Template.C_DDL_TEMPLATE_PATH
        if self.template_type==const('C_ETL').constant_value:
            l_template_path=Template.C_ETL_TEMPLATE_PATH
        if self.template_type==const('C_VIEW').constant_value:
            l_template_path=Template.C_VIEW_TEMPLATE_PATH

        l_template_folder=const('C_DBMS_COMPONENTS').constant_value.get(self.dbms_type,None).get(const('C_TEMPLATE_FOLDER').constant_value,None)
        l_template_path=Path(l_template_path,l_template_folder)

        l_template_path=Path(l_template_path,self.template_name)
        return l_template_path

    @property
    def template_file(self):
        """
        Наполнение шаблона
        """
        l_template_body=FileWorker.File(
            self.table_type,
            self.template_path
        )
        return l_template_body