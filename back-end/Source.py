# coding=utf-8
from SystemObjects import Constant as const
import Metadata
import SQLScript
import MSSQL as mssql
import Postgresql as pgsql
import Support
import pyodbc
import sys
import datetime
import json
import psycopg2
import Driver

class Source:
    """
    Источник
    """
    # TODO:staticmethod - устаревшие методы!

    # проверяет корректность параметров источника
    @staticmethod
    def source_check(source_attr,name_check=None):
        # name=1 - есть необходимость проверить наименование источника
        name=source_attr.get("name",None)
        # проверка подключения
        Driver.Driver.cnct_connect_check(source_attr)
        exist_name=Metadata.Metadata.select_meta("source",None,0,{"name":str(name)})
        if len(exist_name)>0 and name_check==1:
            error = Support.Support.error_output("Source","source_check","Source '"+str(name)+"' already exists")
            sys.exit(error)

    # выдает параметры источника
    @staticmethod
    def select_source(id=None, deleted=None):
        source=Metadata.Metadata.select_meta("source",id,deleted)
        return source

    # добавляем источник
    @staticmethod
    def insert_source(source_attr,tran):
        # проверяем источник (подключение и наименование)
        Source.source_check(source_attr,1)
        # добавляем в метаданные
        if source_attr.get("deleted",None) is None:
            source_attr.update({"deleted":0})
        ins_res = Metadata.Metadata.insert_meta("source",source_attr,tran)
        return ins_res

    # изменяем источник
    @staticmethod
    def update_source(source_id, source_attr, tran):
        # достаем атрибуты
        cur_source_attr=Metadata.Metadata.select_meta("source",source_id, 0)
        if len(cur_source_attr)==0:
            error=Support.Support.error_output("Source","update_source","Source '"+str(source_id)+"' hasn't found")
            sys.exit(error)
        # изменяем атрибуты
        cur_source_attr[0].update(source_attr)
        # проверяем источник (подключение и наименование)
        Source.source_check(cur_source_attr[0])
        # добавляем в метаданные
        ins_res = Metadata.Metadata.update_meta("source",source_id, source_attr,tran)
        return ins_res

    # удаляем источник
    @staticmethod
    def delete_source(source_id, tran):
        delete = Metadata.Metadata.update_meta("source",source_id, {"deleted":"1"},tran)
        return delete

    # вытаскиваем объекты источника (схема, таблица, атрибут, свойства атрибута)
    @staticmethod
    def select_source_object(source_id, object=None):
        # object - database, schema, table
        source_attr=Source.select_source(source_id,0)
        obj_frame=Driver.Driver.select_db_object(source_attr[0],object)
        databases = []
        schemas = []
        tables = []
        attributes = []
        objects = []
        for obj in obj_frame:
            if obj[0] not in databases:
                databases_arr = {}
                databases_arr.update({"database":obj[0],"schemas":[]})
                objects.append(databases_arr)
                databases.append(obj[0])
            if obj[0]+"&"+obj[1] not in schemas:
                for dtbs in objects:
                    if dtbs["database"] == obj[0]:
                        schemas_list = dtbs["schemas"]
                        schemas_list.append({"schema":obj[1], "tables":[]})
                        dtbs["schemas"]=schemas_list
                schemas.append(obj[0]+"&"+obj[1])
            if obj[0]+"&"+obj[1]+obj[2] not in tables:
                for dtbs in objects:
                    if dtbs["database"] == obj[0]:
                        for schms in dtbs["schemas"]:
                            if schms["schema"] == obj[1]:
                                tables_list = schms["tables"]
                                tables_list.append({"table":obj[2],"table_type":obj[3],"attributes":[]})
                                schms["tables"]=tables_list
                tables.append(obj[0]+"&"+obj[1]+obj[2])
            if obj[0]+"&"+obj[1]+obj[2]+"&"+obj[4] not in attributes:
                for dtbs in objects:
                    if dtbs["database"] == obj[0]:
                        for schms in dtbs["schemas"]:
                            if schms["schema"] == obj[1]:
                                for tbls in schms["tables"]:
                                    if tbls["table"] == obj[2]:
                                        attributes_list = tbls["attributes"]
                                        attributes_list.append({"attribute":obj[4],"datatype":obj[5],"key":obj[9]})
                                        tbls["attributes"]=attributes_list
            attributes.append(obj[0]+"&"+obj[1]+obj[2]+"&"+obj[4])
        return objects

    # выдает первые 10 строк таблицы источника
    @staticmethod
    def source_raw(source_id, object):
        table_raw = []
        source_attr=Source.select_source(source_id,0)
        raws=Driver.Driver.select_top_raw(source_attr[0],object)
        columns=Driver.Driver.select_db_object(source_attr[0],object)
        col_num = len(columns)
        raw_num = len(raws)
        test = []
        i = 0
        while i < raw_num:
            raw = {}
            j = 0
            while j < col_num:
                raw.update({columns[j][4]:str(raws[i][j])})
                j = j + 1
            table_raw.append(raw)
            i = i + 1
        return table_raw


    def __init__(self,
                 p_id: str =None,
                 p_name: str =None,
                 p_desc: str =None,
                 p_server: str =None,
                 p_database: str =None,
                 p_user: str =None,
                 p_password: str =None,
                 p_port: int =None,
                 p_type: str =None
    ):
        """
        Конструктор

        :param p_id: id источника
        :param p_name: наименование источника
        :param p_desc: описание источника
        :param p_server: сервер
        :param p_database: бд
        :param p_user: логин
        :param p_password: пароль
        :param p_port: порт
        :param p_type: тип источника
        """
        self._id=p_id
        self._name=p_name
        self._desc=p_desc
        self._server=p_server
        self._database=p_database
        self._user=p_user
        self._password=p_password
        self._type=p_type
        self._port=p_port
        self._deleted=0

        # проверяем, есть ли указанный id и определяем атрибуты из метаданных
        self.__source_meta_attrs=self.__source_meta_attrs()

    def __source_meta_attrs(self):
        """
        Находит атрибуты существующих в метаданных источинков
        """
        l_attr_dict={} # словарь для атрибутов источника
        if self._id is not None:
            l_source_meta_objs=Metadata.search_object(
                p_type=const('C_SOURCE_META').constant_value,
                p_uuid=[self._id]
            ) # достаем метаданные источника
            # проверяет на наличие источника в метаданных
            if l_source_meta_objs.__len__()==0:
                sys.exit("Нет источника с указанным id "+self._id)
            else:
                l_attr_dict=l_source_meta_objs[0].attrs
        return l_attr_dict

    @property
    def id(self):
        """
        Id источника
        """
        return self._id

    @id.setter
    def id(self, p_new_id: str):
        """
        Сеттер id источника

        :param p_new_id: новый id
        """
        if self._id is None:
            self._id=p_new_id

    @property
    def name(self):
        """
        Наименование источника
        """
        return self._name or self.__source_meta_attrs.get(const('C_NAME').constant_value,None)

    @property
    def description(self):
        """
        Описание источника
        """
        return self._desc or self.__source_meta_attrs.get(const('C_DESC').constant_value,None)

    @property
    def server(self):
        """
        Сервер
        """
        return self._server or self.__source_meta_attrs.get(const('C_SERVER').constant_value,None)

    @property
    def database(self):
        """
        База данных
        """
        return self._database or self.__source_meta_attrs.get(const('C_DATABASE').constant_value,None)

    @property
    def user(self):
        """
        Логин
        """
        return self._user or self.__source_meta_attrs.get(const('C_USER').constant_value,None)

    @property
    def password(self):
        """
        Пароль
        """
        return self._password or self.__source_meta_attrs.get(const('C_PASSWORD').constant_value,None)

    @property
    def port(self):
        """
        Порт
        """
        return self._port or self.__source_meta_attrs.get(const('C_PORT').constant_value,None)

    @property
    def source_id(self):
        """
        source_system_id
        """
        # вычисляем максимальный source_id из метаданных
        l_sources=Metadata.search_object(
            p_type=const('C_SOURCE_META').constant_value
        )
        l_source_id_list=[] # список source_id
        for i_source in l_sources:
            l_source_id_list.append(i_source.attrs.get(const('C_SOURCE_ID').constant_value))
        l_first_source_id=None
        if l_source_id_list.__len__()==0:
            l_first_etl_id=1
        return self.__source_meta_attrs.get(const('C_SOURCE_ID').constant_value,None) or l_first_etl_id or max(l_source_id_list)+1

    @property
    def type(self):
        """
        Тип источника
        """
        if self._type is not None and self._type not in const('C_AVAILABLE_SOURCE_LIST').constant_value:
            sys.exit("Некорректный источник "+self._type)
        return self._type or self.__source_meta_attrs.get(const('C_TYPE_VALUE').constant_value,None)

    @property
    def __source_objects(self):
        """
        Получает таблицы и атрибуты источника
        """
        l_source_meta_objects=self.__cnct.get_objects(
            p_server=self.server,
            p_database=self.database,
            p_user=self.user,
            p_password=self.password,
            p_port=self.port
        ) # получаем метаданные источника
        return l_source_meta_objects

    @property
    def source_tables(self):
        """
        Все таблицы источника
        """
        l_source_table=set() # множетво таблиц (не повторяются наименования таблиц)
        for i_source_meta_object in self.__source_objects:
            l_source_table.add(str(i_source_meta_object[0]).lower()+"."+str(i_source_meta_object[1]).lower()) # добавляем в список и приводим к нижнему регистру
        return l_source_table
    @property
    def source_attributes(self):
        """
        Все атрибуты источника
        """
        l_source_attributes=set()
        for i_source_attributes in self.__source_objects:
            l_source_attributes.add(str(i_source_attributes[0]).lower()+"."+str(i_source_attributes[1]).lower()+"."+str(i_source_attributes[4]).lower()) # добавляем в список и приводим к нижнему регистру
        return l_source_attributes


    @property
    def __cnct(self):
        """
        Подключение к источнику
        """
        if self.type==const('C_MSSQL').constant_value:
            return mssql
        if self.type==const('C_POSTGRESQL').constant_value:
            return pgsql

    def sql_exec(self, p_sql: str):
        """
        Выполняет запросы на источнике

        :param p_sql: SQL-запрос
        """
        l_result=self.__cnct.sql_exec(
            p_server=self.server,
            p_database=self.database,
            p_user=self.user,
            p_password=self.password,
            p_port=self.port,
            p_sql=p_sql,
            p_result=1
        )
        return l_result

    def connection_checker(self):
        """
        Проверяет подключение к источнику
        """
        # выполняет простой запрос на источнике
        self.sql_exec(
            p_sql="SELECT 1;"
        )

    @property
    def __meta_obj(self):
        """
        Объект метаданных
        """
        l_source_meta_obj=Metadata.MetaObject(
            p_type=const('C_SOURCE_META').constant_value,
            p_attrs={
                const('C_SERVER').constant_value:self.server,
                const('C_DATABASE').constant_value:self.database,
                const('C_USER').constant_value:self.user,
                const('C_PASSWORD').constant_value:self.password,
                const('C_PORT').constant_value:self.port,
                const('C_TYPE_VALUE').constant_value:self.type,
                const('C_NAME').constant_value:self.name,
                const('C_DESC').constant_value:self.description,
                const('C_SOURCE_ID').constant_value:self.source_id
            },
            p_uuid=self._id
        )
        # проставляем id источника
        self.id=l_source_meta_obj.uuid

        return l_source_meta_obj

    def create_source(self):
        """
        Создает источник в метаданных
        """
        # проверка корректного подключения
        self.connection_checker()
        Metadata.create_object(
            p_object=self.__meta_obj
        )

    def update_source(self):
        """
        Изменяет источник в метаданных
        """
        # проверка корректного подключения
        self.connection_checker()
        Metadata.update_object(
            p_object=self.__meta_obj
        )

    def delete_source(self):
        """
        Удаляет источник
        """
        self.deleted=1
        Metadata.update_object(
            p_object=self.__meta_obj
        )

    @property
    def current_timestamp_sql(self):
        """
        Скрипт текущей даты и времени
        """
        return self.__cnct.C_CURRENT_TIMESTAMP_SQL







