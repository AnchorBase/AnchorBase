# coding=utf-8
import Metadata
import MSSQL as mssql
import Postgresql as pgsql
import MySQL as mysql
import OneC as onec
import Excel as excel
import sys
from Constants import *
import SystemObjects
from SystemObjects import *


class Source:
    """
    Источник
    """

    def __init__(self,
                 p_id: str =None,
                 p_name: str =None,
                 p_desc: str =None,
                 p_server: str =None,
                 p_database: str =None,
                 p_user: str =None,
                 p_password: str =None,
                 p_port: int =None,
                 p_type: str =None,
                 p_file: str =None,
                 p_worksheet: str =None,
                 p_header: bool =None,
                 p_first_row: int =None
    ):
        """
        Конструктор

        :param p_id: source id
        :param p_name: source name
        :param p_desc: source description
        :param p_server: server
        :param p_database: database
        :param p_user: login
        :param p_password: password
        :param p_port: port
        :param p_type: source type
        :param p_file: file (path + name)
        :param p_worksheet: worksheet (for excel only)
        :param p_header: there is a header or not
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
        self._file=p_file
        self._worksheet=p_worksheet
        self._header=p_header
        self._first_row=p_first_row
        # проверяем, есть ли указанный id и определяем атрибуты из метаданных
        self.source_meta_attrs=self.__source_meta_attrs()
        # проверяет полноту указанных атрибутов в соответствии с типом источника
        self.__necessary_cnct_params_check()
        # максимальный source_id
        self._max_source_id=self.__max_source_id()

    def __source_meta_attrs(self):
        """
        Находит атрибуты существующих в метаданных источинков
        """
        l_attr_dict={} # словарь для атрибутов источника
        if self._id is not None:
            l_source_meta_objs=Metadata.search_object(
                p_type=C_SOURCE_META,
                p_uuid=[self._id]
            ) # достаем метаданные источника
            # проверяет на наличие источника в метаданных
            if l_source_meta_objs.__len__()==0:
                AbaseError(p_error_text="There's no source with ID mentioned "+self._id, p_module="Source", p_class="Source",
                           p_def="__source_meta_attrs").raise_error()
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
        return self._name or self.source_meta_attrs.get(C_NAME, None)

    @property
    def description(self):
        """
        Описание источника
        """
        if self._desc=='null': # обработка случаев, когда пользователь сам задал пустое описание
            return None
        else:
            return self._desc or self.source_meta_attrs.get(C_DESC, None)

    @description.setter
    def description(self, p_new_desc: str):
        self._desc=p_new_desc
        self.source_meta_attrs.pop(C_DESC, None)

    @property
    def server(self):
        """
        Сервер
        """
        return self._server or self.source_meta_attrs.get(C_SERVER, None)

    @server.setter
    def server(self, p_new_server: str):
        self._server=p_new_server
        self.source_meta_attrs.pop(C_SERVER, None)

    @property
    def database(self):
        """
        База данных
        """
        return self._database or self.source_meta_attrs.get(C_DATABASE, None)

    @database.setter
    def database(self, p_new_database: str):
        self._database=p_new_database
        self.source_meta_attrs.pop(C_DATABASE, None)

    @property
    def user(self):
        """
        Логин
        """
        return self._user or self.source_meta_attrs.get(C_USER, None)

    @user.setter
    def user(self, p_new_user: str):
        self._user=p_new_user
        self.source_meta_attrs.pop(C_USER, None)

    @property
    def password(self):
        """
        Пароль
        """
        if self._password=='null': # обработка случаев, когда пользователь сам задал пустой пароль
            return None
        else:
            return self._password or self.source_meta_attrs.get(C_PASSWORD, None)

    @password.setter
    def password(self, p_new_password: str):
        self._password=p_new_password
        self.source_meta_attrs.pop(C_PASSWORD, None)

    @property
    def port(self):
        """
        Порт
        """
        return self._port or self.source_meta_attrs.get(C_PORT, None)

    @port.setter
    def port(self, p_new_port: str):
        self._port=p_new_port
        self.source_meta_attrs.pop(C_PORT, None)

    def __max_source_id(self) -> int:
        """
        Вычисляет максимальный source_id в метаданных
        """
        l_sources=Metadata.search_object(
            p_type=C_SOURCE_META
        )
        l_source_id_list=[] # список source_id
        for i_source in l_sources:
            l_source_id_list.append(i_source.attrs.get(C_SOURCE_ID))
        l_first_etl_id=None
        if l_source_id_list.__len__()==0:
            l_first_etl_id=1
        return l_first_etl_id or max(l_source_id_list) + 1

    @property
    def source_id(self):
        """
        source_system_id
        """
        return self.source_meta_attrs.get(C_SOURCE_ID, None) or self._max_source_id

    @property
    def source_type(self)->str:
        """
        Source type
        """
        return C_SOURCE_TYPE.get(self.type)

    @property
    def type(self):
        """
        Тип источника
        """
        if self._type is not None and self._type not in C_AVAILABLE_SOURCE_LIST:
            AbaseError(p_error_text="The source is incorrect: " + self._type, p_module="Source",
                       p_class="Source", p_def="type").raise_error()
        return self._type or self.source_meta_attrs.get(C_TYPE_VALUE, None)

    @property
    def file(self) -> str:
        """
        File (path + name)
        """
        l_file=None
        if self._file is not None:
            l_file=self._file
        return l_file or self.source_meta_attrs.get(C_FILE,None)

    @file.setter
    def file(self, p_new_file: str):
        self._file=p_new_file
        self.source_meta_attrs.pop(C_FILE, None)

    @property
    def worksheet(self) -> str:
        """
        Worksheet (for excel only)
        """
        l_worksheet=None
        if self._worksheet is not None:
            l_worksheet=self._worksheet
        return l_worksheet or self.source_meta_attrs.get(C_WORKSHEET,None)

    @worksheet.setter
    def worksheet(self, p_new_worksheet: str):
        self._worksheet=p_new_worksheet
        self.source_meta_attrs.pop(C_WORKSHEET, None)

    @property
    def header(self) -> bool:
        """
        Sign that table header exists
        """
        l_header=None
        if self._header is not None:
            if str(self._header).lower()=='false':
                l_header=False
            elif str(self._header).lower()=='true':
                l_header=True
            else:
                AbaseError(p_error_text="Unexpected value of header parameter (bool type)", p_module="Source",
                           p_class="Source", p_def="header").raise_error()
        return l_header if l_header is not None else self.source_meta_attrs.get(C_HEADER,None)

    @header.setter
    def header(self, p_new_header: bool):
        self._header=p_new_header
        self.source_meta_attrs.pop(C_HEADER, None)

    @property
    def first_row(self) -> int:
        """
        Number of a first row
        """
        l_first_row=None
        if self._first_row is not None:
            l_first_row=int(self._first_row)
        return l_first_row or self.source_meta_attrs.get(C_FIRST_ROW,None)

    @first_row.setter
    def first_row(self, p_first_row: int):
        self._first_row=p_first_row
        self.source_meta_attrs.pop(C_FIRST_ROW, None)

    # @property
    #     # def __source_objects(self):
    #     #     """
    #     #     Получает таблицы и атрибуты источника
    #     #     """
    #     #     l_source_meta_objects=self.__cnct.get_objects(
    #     #         p_server=self.server,
    #     #         p_database=self.database,
    #     #         p_user=self.user,
    #     #         p_password=self.password,
    #     #         p_port=self.port
    #     #     ) # получаем метаданные источника
    #     #     return l_source_meta_objects

    # @property
    # def source_tables(self):
    #     """
    #     Все таблицы источника
    #     """
    #     l_source_table=set() # множетво таблиц (не повторяются наименования таблиц)
    #     for i_source_meta_object in self.__source_objects:
    #         l_source_table.add(str(i_source_meta_object[0]).lower()+"."+str(i_source_meta_object[1]).lower()) # добавляем в список и приводим к нижнему регистру
    #     return l_source_table
    # @property
    # def source_attributes(self):
    #     """
    #     Все атрибуты источника
    #     """
    #     l_source_attributes=set()
    #     for i_source_attributes in self.__source_objects:
    #         l_source_attributes.add(str(i_source_attributes[0]).lower()+"."+str(i_source_attributes[1]).lower()+"."+str(i_source_attributes[4]).lower()) # добавляем в список и приводим к нижнему регистру
    #     return l_source_attributes


    @property
    def __cnct(self):
        """
        Подключение к источнику
        """
        if self.type==C_MSSQL:
            return mssql
        elif self.type==C_POSTGRESQL:
            return pgsql
        elif self.type==C_MYSQL:
            return mysql
        elif self.type==C_1C:
            return onec
        elif self.type==C_EXCEL:
            return excel

    def sql_exec(self, p_sql: str):
        """
        Execute the SQL-query in the source (only for dbms source)

        :param p_sql: SQL-query
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

    def get_response(self, p_table: str =None, p_attribute_list: list =None):
        """
        Get the response from the web-server (only for web source)

        :param p_table: table/catalog/entity
        :param p_attribute_list: List of attributes to get from web source
        """
        l_result=self.__cnct.get_response(
            p_server=self.server,
            p_database=self.database,
            p_user=self.user,
            p_password=self.password,
            p_port=self.port,
            p_table=p_table,
            p_attribute_list=p_attribute_list
        )
        return l_result

    def get_data(self, p_attribute_list: list =None):
        """
        Get the data from a file

        :param p_attribute_list: list of columns
        """
        l_result=self.__cnct.get_data(
            p_file=self.file,
            p_worksheet=self.worksheet,
            p_columns=p_attribute_list,
            p_header=self.header,
            p_first_row=self.first_row
        )
        return l_result



    def connection_checker(self):
        """
        Check the source connection
        """
        l_rslt=None
        if self.source_type==C_DBMS: # if the source is dbms
            # execute sql-query in the source
            l_rslt=self.sql_exec(
                p_sql="SELECT 1;"
            )
        if self.source_type==C_WEB: # if the source is web
            l_rslt=self.get_response()
        if self.source_type==C_FILE: # if the source is file
            l_rslt=self.get_data()
        if l_rslt[1]:
            AbaseError(p_error_text=l_rslt[1], p_module="Source",
                       p_class="Source", p_def="type").raise_error()


    @property
    def __meta_obj(self):
        """
        Source metadata object
        """
        l_source_meta_obj=None
        if self.type in (C_MSSQL, C_MYSQL, C_POSTGRESQL, C_1C):
            l_source_meta_obj=Metadata.MetaObject(
                p_type=C_SOURCE_META,
                p_attrs={
                    C_SERVER:self.server,
                    C_DATABASE:self.database,
                    C_USER:self.user,
                    C_PASSWORD:self.password,
                    C_PORT:self.port,
                    C_TYPE_VALUE:self.type,
                    C_NAME:self.name,
                    C_DESC:self.description,
                    C_SOURCE_ID:self.source_id
                },
                p_uuid=self._id
            )
        elif self.type in (C_EXCEL):
            l_source_meta_obj=Metadata.MetaObject(
                p_type=C_SOURCE_META,
                p_attrs={
                    C_FILE:self.file,
                    C_WORKSHEET:self.worksheet,
                    C_HEADER:self.header,
                    C_FIRST_ROW: self.first_row,
                    C_TYPE_VALUE:self.type,
                    C_NAME:self.name,
                    C_DESC:self.description,
                    C_SOURCE_ID:self.source_id
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
        Metadata.delete_object(
            p_object=self.__meta_obj
        )

    @property
    def current_timestamp_sql(self):
        """
        Скрипт текущей даты и времени
        """
        return self.__cnct.C_CURRENT_TIMESTAMP_SQL

    def __necessary_cnct_params_check(self):
        """
        Check that all necessary connection parameters are mentioned

        :raises error: if not all connection parameters are mentioned
        """
        if self.type in (C_MSSQL, C_MYSQL, C_POSTGRESQL, C_1C):
            if not self.server or not self.database or not self.port:
                AbaseError(p_error_text="Incomplete list of connection parameters", p_module="Source",
                           p_class="Source", p_def="__necessary_cnct_params_check").raise_error()
        elif self.type in (C_EXCEL):
            if not self.file:
                AbaseError(p_error_text="Incomplete list of connection parameters", p_module="Source",
                           p_class="Source", p_def="__necessary_cnct_params_check").raise_error()







