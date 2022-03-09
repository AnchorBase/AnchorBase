# coding=utf-8
import Metadata
import MSSQL as mssql
import Postgresql as pgsql
import MySQL as mysql
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

        # проверяем, есть ли указанный id и определяем атрибуты из метаданных
        self.source_meta_attrs=self.__source_meta_attrs()
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
        return self._desc or self.source_meta_attrs.get(C_DESC, None)

    @property
    def server(self):
        """
        Сервер
        """
        return self._server or self.source_meta_attrs.get(C_SERVER, None)

    @property
    def database(self):
        """
        База данных
        """
        return self._database or self.source_meta_attrs.get(C_DATABASE, None)

    @property
    def user(self):
        """
        Логин
        """
        return self._user or self.source_meta_attrs.get(C_USER, None)

    @property
    def password(self):
        """
        Пароль
        """
        return self._password or self.source_meta_attrs.get(C_PASSWORD, None)

    @property
    def port(self):
        """
        Порт
        """
        return self._port or self.source_meta_attrs.get(C_PORT, None)

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
    def type(self):
        """
        Тип источника
        """
        if self._type is not None and self._type not in C_AVAILABLE_SOURCE_LIST:
            AbaseError(p_error_text="The source is incorrect: " + self._type, p_module="Source",
                       p_class="Source", p_def="type").raise_error()
        return self._type or self.source_meta_attrs.get(C_TYPE_VALUE, None)

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
        if self.type==C_MSSQL:
            return mssql
        elif self.type==C_POSTGRESQL:
            return pgsql
        elif self.type==C_MYSQL:
            return mysql

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







