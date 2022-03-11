"""
Работа с ХД
"""
import sys
import dwh_config
import Driver
import Postgresql as pgsql
import FileWorker as fw
import copy
import uuid
import Metadata as meta
from Source import Source as Source
import datetime
from Constants import *
import SystemObjects
from SystemObjects import *

class Connection:
    """
    Подключение к ХД
    """
    l_server=dwh_config.server
    l_database=dwh_config.database
    l_user=dwh_config.user
    l_password=dwh_config.password
    l_port=dwh_config.port
    l_dbms_type=dwh_config.dbms_type
    def __init__(self,
                 p_server: str=l_server,
                 p_database: str =l_database,
                 p_user: str =l_user,
                 p_password: str =l_password,
                 p_port: int =l_port,
                 p_dbms_type: str=l_dbms_type
    ):
        """
        Конструктор

        :param p_server: сервер
        :param p_database: наименование бд postgresql
        :param p_user: пользователь
        :param p_password: пароль
        :param p_port: порт (по умолчанию 5432)
        :param p_dbms_type: тип СУБД
        """
        self._server=p_server
        self._database=p_database
        self._user=p_user
        self._password=p_password
        self._port=p_port
        self._dbms_type=p_dbms_type

    @property
    def server(self):
        """
        Хост сервера ХД
        """
        return self._server

    @property
    def database(self):
        """
        Наименование базы данных
        """
        return self._database

    @property
    def user(self):
        """
        Пользователь
        """
        return self._user

    @property
    def password(self):
        """
        Пароль
        """
        return self._password

    @property
    def port(self):
        """
        Порт
        """
        return self._port

    @property
    def dbms_type(self):
        """
        Тип СУБД
        """
        l_dbms_type=self._dbms_type.lower()
        if l_dbms_type not in C_AVAILABLE_DWH_LIST:
            AbaseError(p_error_text="AnchorBase doesn't support DBMS "+l_dbms_type, p_module="DWH", p_class="Connection",
                       p_def="dbms_type").raise_error()
        return l_dbms_type

    @property
    def dbms(self):
        """
        Модуль СУБД
        """
        if self.dbms_type==C_POSTGRESQL:
            return pgsql


    def sql_exec(self, p_sql: str, p_result: int =1, p_rollback: int =0):
        """
        Выполнение запроса на стороне ХД

        :param p_sql: SQL-выражение
        :param p_result: Признак наличия результата запроса (по умолчанию 1)
        :param p_rollback: Признак необходимости отката транзакции в любом случае (по умолчанию 0)
        """
        l_sql_result=self.dbms.sql_exec(
            p_server=self.server,
            p_database=self.database,
            p_user=self.user,
            p_password=self.password,
            p_port=self.port,
            p_sql=p_sql,
            p_result=p_result,
            p_rollback=p_rollback
        )
        return l_sql_result



    def connection_checker(self):
        """
        Тест подключения
        """
        self.dbms.sql_exec(
            p_server=self.server,
            p_database=self.database,
            p_user=self.user,
            p_password=self.password,
            p_port=self.port,
            p_sql="select 1;"
        )

    def set_dwh_param(self):
        """
        Записывает/Изменяет параметры подключения к ХД
        """
        # тест подключения
        self.connection_checker()
        # переписываем dwh_config
        l_config='server = "'+self.server+'"\n' \
                 'database = "'+self.database+'"\n' \
                 'user = "'+self.user+'"\n' \
                 'password = "'+self.password+'"\n' \
                 'port = "'+str(self.port)+'"\n' \
                 'dbms_type = "'+self.dbms_type+'"'
        fw.File(
            p_file_path=C_CONFIG_FILE_PATH,
            p_file_body=l_config
        ).write_file()


def create_dwh_ddl():
    """
    Создает схемы и расширения ХД
    """
    l_dwh=Connection()
    l_sql=""
    for i_schema in C_SCHEMA_LIST:
        l_sql+="DROP SCHEMA IF EXISTS "+'"'+i_schema+'" CASCADE;\n'+"CREATE SCHEMA "+'"'+i_schema+'";'+"\n"
    l_dwh.sql_exec(p_sql=l_sql, p_result=0)



def _class_define(p_class_name: str, p_id: str, p_type: str =None):
    """
    Инициализирует определенный класс

    :param p_class_name: Наименование класса
    :param p_id: id объекта
    """
    if p_class_name.lower()=="entity":
        return Entity(p_id=p_id)
    if p_class_name.lower()=="attribute":
        return Attribute(p_id=p_id, p_type=p_type)
    if p_class_name.lower()=="sourcetable":
        return SourceTable(p_id=p_id)
    if p_class_name.lower()=="idmap":
        return Idmap(p_id=p_id)
    if p_class_name.lower()=="anchor":
        return Anchor(p_id=p_id)
    if p_class_name.lower()=="attributetable":
        return AttributeTable(p_id=p_id)
    if p_class_name.lower()=="tie":
        return Tie(p_id=p_id)
    if p_class_name.lower()=="source":
        return Source(p_id=p_id)
    if p_class_name.lower()=="package":
        return Package(p_id=p_id, p_type=p_type)
    if p_class_name.lower()=="job":
        return Job(p_id=p_id)

def _get_object(p_id, p_class_name: str, p_type: str=None):
    """
    Достает сущность/сущности из метаданных

    :param p_id: id/список id сущности
    """
    if not p_id: # возвращаем пусто, если ничего не пришло
        return None
    if type(p_id).__name__.lower() in ["uuid","str"]: # если пришел один id
        return _class_define(p_class_name=p_class_name,p_id=p_id, p_type=p_type)
    if type(p_id).__name__.lower()=="list": # если пришел лист из id
        l_entity=[]
        for i_id in p_id:
            if i_id:
                l_entity.append(
                    _class_define(p_class_name=p_class_name,p_id=i_id, p_type=p_type)
                )
        if l_entity.__len__()==0: # если не найдено ни одной сущности
            return None
        return l_entity


def get_values_sql(p_data_frame: list, p_cast_dict: dict, p_etl_id: int):
    """
    Возвращает преобразованный data frame в конструкцию values (...,...),

    :param p_data_frame: data frame
    :param p_cast_list: словарь с порядковыми номерами атрибутов и конструкцией cast для каждого атрибута
    :param p_etl_id: id etl процесса
    """
    l_sql=""
    for i_row in p_data_frame:
        i=0
        l_sql=l_sql+"(\n\t"
        for i_column in i_row:
            if p_cast_dict.get(i,None) is None:
                AbaseError(p_error_text="cast is necessary for " + l_dbms_type, p_module="DWH", p_class="",
                           p_def="get_values_sql").raise_error()
            if not i_column: # преобразуем none в null, если значение пустое
                i_column="NULL "
            else:
                i_column="'"+str(i_column).replace("'","''")+"' " # экранируем одинарную кавычку ' в строке
            l_sql=l_sql+p_cast_dict.get(i,None).replace(str(i),i_column,1)+",\n\t"
            i=i+1
        l_sql=l_sql+"CAST("+str(p_etl_id)+" AS BIGINT)\n),\n" # добавляем etl_id
    return l_sql[:-2]

def _object_class_checker(p_object: object, p_class_name: str):
    """
    Проверяет наименование класса объекта

    :param p_object: объект класса
    """
    if type(p_object).__name__.lower()!=p_class_name.lower():
        AbaseError(p_error_text="Object doesn't belong to class " + p_class_name, p_module="DWH", p_class="",
                   p_def="_object_class_checker").raise_error()


def _class_checker(p_object, p_class_name: str):
    """
    Проверяет наименование у объекта/листа объектов

    :param p_object: объект/лист объектов
    :param p_class_name: наименование класса
    """
    if not p_object: # возвращаем пусто, если ничего не пришло
        return None
    if type(p_object).__name__.lower()=="list": # если передан лист - проверяем каждый объект отдельно
        for i_object in p_object:
            if not i_object: # если пусто - ошибка
                AbaseError(p_error_text="Object doesn't belong to class " + p_class_name, p_module="DWH",
                           p_class="Connection", p_def="_class_checker").raise_error()
            _object_class_checker(p_object=i_object, p_class_name=p_class_name)
    else:
        _object_class_checker(p_object=p_object, p_class_name=p_class_name) # в остальных случаях проверяем объект


def _get_attribute_type(p_attribute: object):
    """
    Получает
    :param p_attribute:
    """
    if not p_attribute:
        AbaseError(p_error_text="Attribute is empty", p_module="DWH",
                   p_class="Connection", p_def="_get_attribute_type").raise_error()
    if p_attribute.type==C_ENTITY_COLUMN:
        return "entity_attribute"
    if p_attribute.type==C_QUEUE_COLUMN:
        return "source_attribute"
    if p_attribute.type==C_IDMAP_COLUMN:
        return "idmap_attribute"
    if p_attribute.type==C_ANCHOR_COLUMN:
        return "anchor_attribute"
    if p_attribute.type==C_ATTRIBUTE_COLUMN:
        return "attribute_table_attribute"
    if p_attribute.type==C_TIE_COLUMN:
        return "tie_attribute"
    if p_attribute.type==C_QUEUE_ETL:
        return "source_table_package"
    if p_attribute.type==C_IDMAP_ETL:
        return "idmap_package"
    if p_attribute.type==C_ANCHOR_ETL:
        return "anchor_package"
    if p_attribute.type==C_ATTRIBUTE_ETL:
        return "attribute_table_package"
    if p_attribute.type==C_TIE_ETL:
        return "tie_package"

def _get_table_attribute_property(p_table: object):
    """
    Получает атрибуты таблицы в зависимости от ее типа

    :param p_table: таблица
    """
    if p_table.type==C_QUEUE:
        return getattr(p_table, "source_attribute")
    if p_table.type==C_IDMAP:
        return getattr(p_table, "idmap_attribute")
    if p_table.type==C_ANCHOR:
        return getattr(p_table, "anchor_attribute")
    if p_table.type==C_ATTRIBUTE_TABLE:
        return getattr(p_table, "attribute_table_attribute")
    if p_table.type==C_TIE:
        return getattr(p_table, "tie_attribute")
    if p_table.type==C_ENTITY:
        return getattr(p_table, "entity_attribute")

def _get_table_property(p_table: object):
    """
    Получает наименование свойства таблицы в зависимости от ее типа

    :param p_table: таблица
    """
    if p_table.type==C_QUEUE:
        return "source_table"
    if p_table.type==C_IDMAP:
        return "idmap"
    if p_table.type==C_ANCHOR:
        return "anchor"
    if p_table.type==C_ATTRIBUTE_TABLE:
        return "attribute_table"
    if p_table.type==C_TIE:
        return "tie_table"
    if p_table.type==C_ENTITY:
        return "entity"

def _add_object(p_parent_object: object, p_object: object, p_object_type: str):
    """
    Добавляет объект в родительский объект

    :param p_parent_object: родительский объект
    :param p_object: добавляемый объект
    :param p_object_type: тип добавляемого объекта (наименование атрибута класса)
    :return:
    """

    l_object=getattr(p_parent_object,p_object_type)
    if type(l_object).__name__=="list": # если уже есть атрибуты - добавляем в список
        l_object.append(p_object)
        setattr(p_parent_object,p_object_type,l_object)# добавление дочернего элемента
    elif type(l_object).__name__=="object": # если атрибут есть, но он один - ошибка
        AbaseError(p_error_text="Can not add more than one attribute", p_module="DWH", p_class="",
                   p_def="_add_object").raise_error()
    elif not l_object: # если нет атрибутов - добавляем список
        setattr(p_parent_object,p_object_type,[p_object])

def add_table(p_object: object, p_table: object):
    """
    Добавляет таблицу объекту (таблица или атрибут)

    :param p_object: объект (таблица/атрибут)
    :param p_table: добавляемая табица
    """
    p_table_property=_get_table_property(p_table=p_table)
    _add_object(
        p_parent_object=p_object,
        p_object=p_table,
        p_object_type=p_table_property
    )



def add_attribute(p_table: object, p_attribute: object, p_add_table_flg: int =1):
    """
    Добавляет дочерний объект в родительский объект

    :param p_table: дочерний объект
    :param p_attribute_list: лист из дочерних объектов уже добавленных в родительский объект
    :param p_add_table_flg: признак необходимости также добавить таблицу в атрибут (по умолчанию - да)
    """
    # добавляем атрибут в таблицу
    _add_object(
        p_parent_object=p_table,
        p_object=p_attribute,
        p_object_type=_get_attribute_type(p_attribute)
    )
    if p_add_table_flg==1:
        # добавляем таблицу в атрибут
        setattr(p_attribute, _get_table_property(p_table=p_table), p_table)

def add_source_to_object(p_object: object, p_source: object):
    """
    Добавляет источник
    :param p_object: объект, в который добавляется источник
    :param p_source: источник (объект класса Source)
    """
    _add_object(
        p_parent_object=p_object,
        p_object=p_source,
        p_object_type=C_SOURCE
    )



def remove_attribute(p_table: object, p_attribute: object):
    """
    Убирает дочерний объект у родительского

    :param p_object: дочерний объект
    :param p_class_name: наименование класса дочернего объекта
    :param p_object_list: список дочерних объектов: из которого нужно убрать заданный дочерний объект
    """
    # проверка
    _object_class_checker(p_class_name="Attribute", p_object=p_attribute)
    l_attribute_list=p_table.attribute
    i=0
    for i_attribute in l_attribute_list:
        if i_attribute.id==p_attribute.id: # если id совпадает
            l_attribute_list.pop(i) # убираем атрибут в соответствии с порядковым номером в листе
        i=i+1
    if l_attribute_list.__len__()==0:
        AbaseError(p_error_text="All attributes already deleted", p_module="DWH", p_class="",
                   p_def="remove_attribute").raise_error()
    p_table.attribute=l_attribute_list


def get_attribute(p_attribute_id_list: list, p_type: str):
    """
    Выдает лист объектов класса Attribute на основе id атрибутов

    :param p_parent_object_id:
    :return:
    """
    l_attributes=[]
    if p_attribute_id_list is not None:
        for i_attribute in p_attribute_id_list:
            l_attributes.append(
                Attribute(
                    p_id=i_attribute,
                    p_type=p_type
                )
            )
    return l_attributes

def _get_table_postfix(p_table_type: str):
    """
    Возвращает постфикс таблицы

    :param p_table_type: тип таблицы
    """
    return C_TABLE_NAME_POSTFIX.get(p_table_type,None)

def drop_table_ddl(p_table: object):
    """
    генерирует скрипт удаления таблицы

    :param p_table: таблица
    """
    l_schema=C_SCHEMA_TABLE_TYPE.get(p_table.type,None) # схема таблицы
    l_sql="DROP TABLE IF EXISTS"+'"'+str(l_schema)+'"'+"."+'"'+str(p_table.id)+'"'+" CASCADE;"
    return l_sql


def create_table_ddl(p_table: object):
    """
    Генерирует DDL таблицы
    :param p_table: таблица
    """
    l_schema=C_SCHEMA_TABLE_TYPE.get(p_table.type,None) # схема таблицы
    p_attribute_sql="" # перечисление атрибутов
    p_attribute_partition_sql="" # указание атрибута для партиции
    for i_attribute in _get_table_attribute_property(p_table=p_table): # проходимся по всем атрибутам и формируем ddl
        p_attribute_sql=p_attribute_sql+"\n"+'"'+str(i_attribute.id)+'"'+" "+i_attribute.datatype.data_type_sql+","
        if i_attribute.attribute_type==C_FROM:
            p_attribute_partition_sql="\nPARTITION BY RANGE ("+'"'+str(i_attribute.id)+'"'+")"
    p_attribute_sql=p_attribute_sql[:-1] # убираем последнюю запятую
    p_attribute_sql=p_attribute_sql[1:] # убираем первый перенос строк
    l_sql="CREATE TABLE "+l_schema+"."+'"'+str(p_table.id)+'"'+"(\n"+p_attribute_sql+"\n"+")"+p_attribute_partition_sql+";"
    return l_sql

def create_view_ddl(p_table: object):
    """
    Генерирует DDL представления

    :param p_table: таблица
    """
    l_table_name=None
    if p_table.type==C_QUEUE: # для таблицы queue другой атрибут объекта в качестве имени
        l_table_name=p_table.queue_name
    else:
        l_table_name=p_table.name
    l_schema=C_SCHEMA_TABLE_TYPE.get(p_table.type,None) # схема таблицы
    p_attribute_sql="" # перечисление атрибутов
    for i_attribute in _get_table_attribute_property(p_table=p_table):
        p_attribute_sql=p_attribute_sql+"\n"+'"'+str(i_attribute.id)+'"'+" AS "+'"'+i_attribute.name+'"'+","
    p_attribute_sql=p_attribute_sql[:-1] # убираем последнюю запятую
    p_attribute_sql=p_attribute_sql[1:] # убираем первый перенос строк
    l_sql="CREATE OR REPLACE VIEW "+l_schema+"."+'"'+l_table_name+'"'+" AS \nSELECT\n"+p_attribute_sql\
          +"\nFROM "+l_schema+"."+'"'+str(p_table.id)+'";'
    return l_sql

def drop_view_ddl(p_table: object):
    """
    Генерирует скрипт удаления представления

    :param p_table: таблица
    """
    l_table_name=None
    if p_table.type==C_QUEUE: # для таблицы queue другой атрибут объекта в качестве имени
        l_table_name=p_table.queue_name
    else:
        l_table_name=p_table.name
    l_schema=C_SCHEMA_TABLE_TYPE.get(p_table.type,None) # схема таблицы
    l_sql="DROP VIEW IF EXISTS "+l_schema+"."+'"'+l_table_name+'";'
    return l_sql

def get_source_table_etl(p_source_table: object, p_attribute_value: str):
    """
    ETL таблицы источника

    :param p_source_table: объект класса SourceTable
    :param p_etl: id etl процесса
    :param p_attribute_value: значения атрибутов, полученные с источника и преобразованные для вставки
    """
    l_attribute_name_list=[]
    l_update_attribute_sql=""
    l_etl_update_sql=""
    for i_attribute in _get_table_attribute_property(p_table=p_source_table):
        if i_attribute.attribute_type not in ( # атрибуты etl и update добавляются в конец
                [
                    C_ETL_ATTR,
                    C_UPDATE
                ]
        ):
            l_attribute_name_list.append(i_attribute.name)
        elif i_attribute.attribute_type==C_UPDATE:
            l_update_attribute_sql='"'+str(i_attribute.id)+'"'
        elif i_attribute.attribute_type==C_ETL_ATTR:
            l_etl_update_sql='"'+str(i_attribute.id)+'"'
    l_attribute_name_list.sort() # сортируем по наименованию
    # сортируем id в соответствии с наименованием атрибутов
    l_attribute_sql="\n\t\t"
    for i_attribute_name in l_attribute_name_list:
        for i_attribute in _get_table_attribute_property(p_table=p_source_table):
            if i_attribute.name==i_attribute_name:
                l_attribute_sql=l_attribute_sql+'"'+str(i_attribute.id)+'"'+",\n\t\t"
    l_attribute_sql=l_attribute_sql+l_update_attribute_sql+",\n\t\t"+l_etl_update_sql+"\n\t"

    l_etl=Connection().dbms.get_source_table_etl(
        p_source_table_id=p_source_table.id,
        p_source_attribute=l_attribute_sql,
        p_source_attribute_value=p_attribute_value
    )
    return l_etl

def get_idmap_etl(
        p_idmap: object,
        p_etl_id: str,
        p_source_table: object =None
):
    """
    Генерирует скрипт ETL для таблицы Idmap

    :param p_idmap: объект класса Idmap
    :param p_etl_id: id etl процесса
    :param p_source_table: таблица источник, которую требуется загрузить в idmap
                           (если не указана, возвращается лист с etl всех таблиц источников)
    """
    l_source_table_id=None
    if p_source_table:
        l_source_table_id=p_source_table.id
    l_etl=[]
    l_idmap_nk_column=None
    l_idmap_rk_column=None
    l_etl_column=None
    for i_attribute in _get_table_attribute_property(p_table=p_idmap):
        if i_attribute.attribute_type==C_RK:
            l_idmap_rk_column=i_attribute.id
        if i_attribute.attribute_type==C_NK:
            l_idmap_nk_column=i_attribute.id
        if i_attribute.attribute_type==C_ETL_ATTR:
            l_etl_column=i_attribute.id
    for i_source_table in p_idmap.entity.source_table:
        if l_source_table_id and l_source_table_id!=i_source_table.id: # пропускаем таблицу источник, если не она указана
            continue
        l_column_nk_sql=""
        # формируем скрипт для конкатенации натуральных ключей
        # сортируем список натуральных ключей по наименованию
        l_source_attribute_nk=sorted(p_idmap.source_attribute_nk, key=lambda nk: nk.name)
        for i_column_nk in l_source_attribute_nk:
            if i_source_table.id==i_column_nk.source_table.id:
                l_column_nk_sql=l_column_nk_sql+"CAST("+'"'+str(i_column_nk.id)+'"'+" AS VARCHAR(4000))\n\t\t||'@@'||\n\t\t"
        l_column_nk_sql=l_column_nk_sql[:-14]
        l_source_id=i_source_table.source.source_id
        # генерируем etl для каждой таблицы источника
        l_etl.append(
            Connection().dbms.get_idmap_etl(
                p_idmap_id=p_idmap.id,
                p_idmap_rk_id=l_idmap_rk_column,
                p_idmap_nk_id=l_idmap_nk_column,
                p_etl_id=l_etl_column,
                p_etl_value=p_etl_id,
                p_source_table_id=i_source_table.id,
                p_attribute_nk=l_column_nk_sql,
                p_source_id=l_source_id
            )
        )
    return l_etl

def get_anchor_etl(
        p_anchor: object,
        p_etl_id: str
):
    """
    Генерирует ETL для якорной таблицы

    :param p_anchor: объект класса Anchor
    :param p_etl_id: id etl процесса
    """
    l_anchor_rk_id=None
    l_anchor_source_id=None
    l_anchor_etl_id=None
    l_idmap_rk_id=None
    l_idmap_nk_id=None
    for i_attribute in _get_table_attribute_property(p_table=p_anchor):
        if i_attribute.attribute_type==C_RK:
            l_anchor_rk_id=i_attribute.id
        if i_attribute.attribute_type==C_SOURCE_ATTR:
            l_anchor_source_id=i_attribute.id
        if i_attribute.attribute_type==C_ETL_ATTR:
            l_anchor_etl_id=i_attribute.id
    for i_attribute in p_anchor.entity.idmap.idmap_attribute:
        if i_attribute.attribute_type==C_RK:
            l_idmap_rk_id=i_attribute.id
        if i_attribute.attribute_type==C_NK:
            l_idmap_nk_id=i_attribute.id

    return Connection().dbms.get_anchor_etl(
        p_anchor_id=p_anchor.id,
        p_anchor_rk_id=l_anchor_rk_id,
        p_anchor_source_id=l_anchor_source_id,
        p_anchor_etl_id=l_anchor_etl_id,
        p_idmap_id=p_anchor.entity.idmap.id,
        p_idmap_rk_id=l_idmap_rk_id,
        p_idmap_nk_id=l_idmap_nk_id,
        p_etl_value=p_etl_id
    )

def get_attribute_etl(p_attribute_table: object, p_etl_id: str, p_source_table: object =None):
    """
    Генерирует etl скрипт для таблицы attribute

    :param p_attribute_table: объект класса  AttributeTable
    :param p_etl_id: id etl процесса
    :param p_source_table: таблица источник, которую требуется загрузить в attribute
                           (если не указана, возвращается лист с etl всех таблиц источников)
    """
    l_etl=[]
    l_source_table_id=None
    if p_source_table:
        l_source_table_id=p_source_table.id
    l_attribute_rk_id=None
    l_attribute_column_id=None
    l_from_dttm_id=None
    l_to_dttm_id=None
    l_etl_id=None
    l_idmap_rk_id=None
    l_idmap_nk_id=None
    l_column_value_datatype=None
    for i_attribute in _get_table_attribute_property(p_table=p_attribute_table):
        if i_attribute.attribute_type==C_RK:
            l_attribute_rk_id=i_attribute.id
        if i_attribute.attribute_type==C_VALUE:
            l_attribute_column_id=i_attribute.id
            l_column_value_datatype=i_attribute.datatype.data_type_sql
        if i_attribute.attribute_type==C_FROM:
            l_from_dttm_id=i_attribute.id
        if i_attribute.attribute_type==C_TO:
            l_to_dttm_id=i_attribute.id
        if i_attribute.attribute_type==C_ETL_ATTR:
            l_etl_id=i_attribute.id
    for i_attribute in p_attribute_table.entity.idmap.idmap_attribute:
        if i_attribute.attribute_type==C_RK:
            l_idmap_rk_id=i_attribute.id
        if i_attribute.attribute_type==C_NK:
            l_idmap_nk_id=i_attribute.id
    for i_source_attribute in p_attribute_table.entity_attribute.source_attribute:
        if l_source_table_id and l_source_table_id!=i_source_attribute.source_table.id: # пропускаем таблицу источник, если не она указана
            continue
        l_column_nk_sql=""
        l_source_attribute_nk=sorted(p_attribute_table.entity.idmap.source_attribute_nk, key=lambda nk: nk.name)
        for i_column_nk in l_source_attribute_nk:
            if i_source_attribute.source_table.id==i_column_nk.source_table.id:
                l_column_nk_sql=l_column_nk_sql+"CAST("+'"'+str(i_column_nk.id)+'"'+" AS VARCHAR(4000))\n\t\t||'@@'||\n\t\t"
        l_column_value_id=i_source_attribute.id
        l_update_timestamp_id=None
        for i_source_attribute_all in i_source_attribute.source_table.source_attribute:
            if i_source_attribute_all.attribute_type==C_UPDATE:
                l_update_timestamp_id=i_source_attribute_all.id
        l_column_nk_sql=l_column_nk_sql[:-14]
        l_source_id=i_source_attribute.source_table.source.source_id
        # генерируем etl для каждой таблицы источника
        l_etl.append(
            Connection().dbms.get_attribute_etl(
                p_attribute_id=p_attribute_table.id,
                p_attribute_column_id=l_attribute_column_id,
                p_anchor_rk_id=l_attribute_rk_id,
                p_from_dttm_id=l_from_dttm_id,
                p_to_dttm_id=l_to_dttm_id,
                p_etl_id=l_etl_id,
                p_idmap_id=p_attribute_table.entity.idmap.id,
                p_idmap_rk_id=l_idmap_rk_id,
                p_idmap_nk_id=l_idmap_nk_id,
                p_stg_table_id=i_source_attribute.source_table.id,
                p_stg_attribute_id=l_column_value_id,
                p_data_type=l_column_value_datatype,
                p_attribute_concat_nk=l_column_nk_sql,
                p_update_timestamp_id=l_update_timestamp_id,
                p_source_id=l_source_id,
                p_etl_value=p_etl_id
            )
        )
        return l_etl

def get_tie_etl(
        p_tie: object,
        p_etl_id: str,
        p_source_table: object =None
):
    """
    Генерирует etl для таблицы Tie

    :param p_tie: объект класса Tie
    :param p_etl: id etl процесса
    :param p_source_table: таблица источник, которую требуется загрузить в attribute
                           (если не указана, возвращается лист с etl всех таблиц источников)
    """
    l_etl=[]
    l_source_table_id=None
    if p_source_table:
        l_source_table_id=p_source_table.id
    l_anchor_rk_id=None
    l_link_anchor_rk_id=None
    l_from_dttm_id=None
    l_to_dttm_id=None
    l_etl_id=None
    l_idmap_rk_id=None
    l_idmap_nk_id=None
    l_link_idmap_rk_id=None
    l_link_idmap_nk_id=None
    for i_attribute in _get_table_attribute_property(p_table=p_tie):
        if i_attribute.attribute_type==C_RK:
            l_anchor_rk_id=i_attribute.id
        if i_attribute.attribute_type==C_LINK_RK:
            l_link_anchor_rk_id=i_attribute.id
        if i_attribute.attribute_type==C_FROM:
            l_from_dttm_id=i_attribute.id
        if i_attribute.attribute_type==C_TO:
            l_to_dttm_id=i_attribute.id
        if i_attribute.attribute_type==C_ETL_ATTR:
            l_etl_id=i_attribute.id
    for i_attribute in p_tie.entity.idmap.idmap_attribute:
        if i_attribute.attribute_type==C_RK:
            l_idmap_rk_id=i_attribute.id
        if i_attribute.attribute_type==C_NK:
            l_idmap_nk_id=i_attribute.id
    for i_attribute in p_tie.link_entity.idmap.idmap_attribute:
        if i_attribute.attribute_type==C_RK:
            l_link_idmap_rk_id=i_attribute.id
        if i_attribute.attribute_type==C_NK:
            l_link_idmap_nk_id=i_attribute.id
    for i_source_table in p_tie.source_table:
        if l_source_table_id and l_source_table_id!=i_source_table.id:
            continue # пропускаем таблицу источник, если не она указана
        l_column_nk_sql=""
        l_source_attribute_nk=sorted(p_tie.entity.idmap.source_attribute_nk, key=lambda nk: nk.name)
        for i_column_nk in l_source_attribute_nk:
            if i_source_table.id==i_column_nk.source_table.id:
                l_column_nk_sql=l_column_nk_sql+"CAST("+'"'+str(i_column_nk.id)+'"'+" AS VARCHAR(4000))\n\t\t||'@@'||\n\t\t"
        l_link_column_nk_sql=""
        for i_column_nk in p_tie.entity_attribute.source_attribute:
            if i_source_table.id==i_column_nk.source_table.id:
                l_link_column_nk_sql=l_link_column_nk_sql+"CAST("+'"'+str(i_column_nk.id)+'"'+" AS VARCHAR(4000))\n\t\t||'@@'||\n\t\t"
        l_update_timestamp_id=None
        for i_source_attribute in i_source_table.source_attribute:
            if i_source_attribute.attribute_type==C_UPDATE:
                l_update_timestamp_id=i_source_attribute.id
        l_column_nk_sql=l_column_nk_sql[:-14]
        l_link_column_nk_sql=l_link_column_nk_sql[:-14]
        # генерируем etl для каждой таблицы источника
        l_etl.append(
            Connection().dbms.get_tie_etl(
                p_tie_id=p_tie.id,
                p_anchor_rk=l_anchor_rk_id,
                p_link_anchor_rk=l_link_anchor_rk_id,
                p_from_dttm_id=l_from_dttm_id,
                p_to_dttm_id=l_to_dttm_id,
                p_etl_id=l_etl_id,
                p_idmap_id=p_tie.entity.idmap.id,
                p_idmap_rk_id=l_idmap_rk_id,
                p_idmap_nk_id=l_idmap_nk_id,
                p_idmap_concat=l_column_nk_sql,
                p_link_idmap_id=p_tie.link_entity.idmap.id,
                p_link_idmap_rk_id=l_link_idmap_rk_id,
                p_link_idmap_nk_id=l_link_idmap_nk_id,
                p_link_idmap_concat=l_link_column_nk_sql,
                p_stg_table_id=i_source_table.id,
                p_update_timestamp_id=l_update_timestamp_id,
                p_source_id=i_source_table.source.source_id,
                p_etl_value=p_etl_id
            )
        )
        return l_etl

def get_max_etl_id():
    """
    Возвращает максимальный etl_id
    """
    # вычисляем максимальный etl_id из метаданных
    l_etls=meta.search_object(
        p_type=C_ETL
    )
    l_etl_id_list=[0] # список etl_id, начинается с 0, так как в метаданных может не быть etl_id
    for i_etl in l_etls:
        l_etl_id_list.append(i_etl.attrs.get(C_ETL_ATTRIBUTE_NAME))
    return max(l_etl_id_list)

class _DWHObject:
    """
    Обхъект ХД
    """

    def __init__(self,
                 p_type: str,
                 p_id: str =None,
                 p_name: str =None,
                 p_source_table =None,
                 p_source_attribute =None,
                 p_entity =None,
                 p_entity_attribute =None,
                 p_link_entity =None,
                 p_idmap =None,
                 p_idmap_attribute =None,
                 p_anchor =None,
                 p_anchor_attribute =None,
                 p_attribute_table =None,
                 p_attribute_table_attribute =None,
                 p_tie =None,
                 p_tie_attribute =None,
                 p_desc: str =None,
                 p_source =None
    ):
        """
        Конструктор

        :param p_type: тип объекта ХД
        :param p_id: id объекта ХД
        """
        self._type=p_type
        self._id=p_id
        self.l_id=copy.copy(uuid.uuid4()) # чтобы id не изменялся при каждом вызове
        self._source_table=p_source_table
        self._source_attribute=p_source_attribute
        self._entity=p_entity
        self._entity_attribute=p_entity_attribute
        self._link_entity=p_link_entity
        self._idmap=p_idmap
        self._idmap_attribute=p_idmap_attribute
        self._anchor=p_anchor
        self._anchor_attribute=p_anchor_attribute
        self._attribute_table=p_attribute_table
        self._attribute_table_attribute=p_attribute_table_attribute
        self._tie=p_tie
        self._tie_attribute=p_tie_attribute
        self._desc=p_desc
        self._source=p_source
        self._name=p_name

        # проверяем, есть ли указанный id и определяем атрибуты из метаданных
        self.object_attrs_meta=self.__object_attrs_meta()


    @property
    def id(self):
        """
        Id объекта ХД
        :return:
        """
        if self._id is None:
            return self.l_id
        else:
            return self._id

    @property
    def name(self):
        """
        Наименование
        """
        l_name=None
        if self._name is not None:
            l_name=self._name.lower()
        return l_name or self.object_attrs_meta.get(C_NAME,None)

    @name.setter
    def name(self, p_new_name: str):
        self._name=p_new_name
        self.object_attrs_meta.pop(C_NAME, None)

    def __object_attrs_meta(self):
        """
        Атрибуты объекта ХД из метаданных
        """
        l_attr_dict={} # словарь для атрибутов из метаданных
        if self._id is not None:
            l_meta_objs=meta.search_object(
                p_type=self._type,
                p_uuid=[self._id]
            ) # достаем метаданные источника
            # проверяет на наличие источника в метаданных
            if l_meta_objs.__len__()==0:
                AbaseError(p_error_text="There's no "+self._type+" with id "+self._id, p_module="DWH", p_class="_DWHObject",
                           p_def="__object_attrs_meta").raise_error()
            else:
                l_attr_dict=l_meta_objs[0].attrs
        return l_attr_dict

    @property
    def type(self):
        """
        Тип объекта
        """
        return self._type

    @property
    def source(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Source",p_object=self._source)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_SOURCE,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Source")

        return self._source or l_object

    @source.setter
    def source(self, p_new_source):

        self._source=p_new_source
        self.object_attrs_meta.pop(C_SOURCE, None)

    @property
    def source_table(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="SourceTable",p_object=self._source_table)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_QUEUE, None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="SourceTable")

        return self._source_table or l_object

    @source_table.setter
    def source_table(self, p_new_source_table):

        self._source_table=p_new_source_table
        self.object_attrs_meta.pop(C_QUEUE, None)

    @property
    def source_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._source_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_QUEUE_COLUMN,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute", p_type=C_QUEUE_COLUMN)

        return self._source_attribute or l_object

    @source_attribute.setter
    def source_attribute(self, p_new_source_attribute):

        self._source_attribute=p_new_source_attribute
        self.object_attrs_meta.pop(C_QUEUE_COLUMN, None)

    @property
    def entity(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Entity",p_object=self._entity)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ENTITY,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Entity")

        return self._entity or l_object

    @entity.setter
    def entity(self, p_new_entity):

        self._entity=p_new_entity
        self.object_attrs_meta.pop(C_ENTITY, None)

    @property
    def link_entity(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Entity",p_object=self._link_entity)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_LINK_ENTITY,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Entity")

        return self._link_entity or l_object

    @link_entity.setter
    def link_entity(self, p_new_link_entity):

        self._link_entity=p_new_link_entity
        self.object_attrs_meta.pop(C_LINK_ENTITY, None)

    @property
    def entity_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._entity_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ENTITY_COLUMN,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=C_ENTITY_COLUMN)

        return self._entity_attribute or l_object

    @entity_attribute.setter
    def entity_attribute(self, p_new_entity_attribute):

        self._entity_attribute=p_new_entity_attribute
        self.object_attrs_meta.pop(C_ENTITY_COLUMN, None)

    @property
    def idmap(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Idmap",p_object=self._idmap)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_IDMAP, None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Idmap")

        return self._idmap or l_object

    @idmap.setter
    def idmap(self, p_new_idmap):

        self._idmap=p_new_idmap
        self.object_attrs_meta.pop(C_IDMAP, None)

    @property
    def idmap_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._idmap_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_IDMAP_COLUMN,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=C_IDMAP_COLUMN)

        return self._idmap_attribute or l_object

    @idmap_attribute.setter
    def idmap_attribute(self, p_new_idmap_attribute):

        self._idmap_attribute=p_new_idmap_attribute
        self.object_attrs_meta.pop(C_IDMAP_COLUMN, None)

    @property
    def anchor(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Anchor",p_object=self._anchor)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ANCHOR, None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Anchor")

        return self._anchor or l_object

    @anchor.setter
    def anchor(self, p_new_anchor):

        self._anchor=p_new_anchor
        self.object_attrs_meta.pop(C_ANCHOR, None)

    @property
    def anchor_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._anchor_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ANCHOR_COLUMN,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=C_ANCHOR_COLUMN)

        return self._anchor_attribute or l_object

    @anchor_attribute.setter
    def anchor_attribute(self, p_new_anchor_attribute):

        self._anchor_attribute=p_new_anchor_attribute
        self.object_attrs_meta.pop(C_ANCHOR_COLUMN, None)

    @property
    def attribute_table(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="AttributeTable",p_object=self._attribute_table)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ATTRIBUTE_TABLE, None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="AttributeTable")

        return self._attribute_table or l_object

    @attribute_table.setter
    def attribute_table(self, p_new_attribute_table):

        self._attribute_table=p_new_attribute_table
        self.object_attrs_meta.pop(C_ATTRIBUTE_TABLE, None)

    @property
    def attribute_table_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._attribute_table_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ATTRIBUTE_COLUMN,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=C_ATTRIBUTE_COLUMN)

        return self._attribute_table_attribute or l_object

    @attribute_table_attribute.setter
    def attribute_table_attribute(self, p_new_attribute_table_attribute):

        self._attribute_table_attribute=p_new_attribute_table_attribute
        self.object_attrs_meta.pop(C_ATTRIBUTE_COLUMN, None)

    @property
    def tie(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Tie",p_object=self._tie)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_TIE, None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Tie")

        return self._tie or l_object

    @tie.setter
    def tie(self, p_new_tie):

        self._tie=p_new_tie
        self.object_attrs_meta.pop(C_TIE, None)

    @property
    def tie_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._tie_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_TIE_COLUMN,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=C_TIE_COLUMN)

        return self._tie_attribute or l_object

    @tie_attribute.setter
    def tie_attribute(self, p_new_tie_attribute):

        self._tie_attribute=p_new_tie_attribute
        self.object_attrs_meta.pop(C_TIE_COLUMN, None)

    @property
    def desc(self):
        """
        Описание атрибута
        """
        if self._desc=='null':
            return None
        else:
            return self._desc or self.object_attrs_meta.get(C_DESC,None)

    @desc.setter
    def desc(self, p_new_desc):

        self._desc=p_new_desc if p_new_desc!='null' else None
        self.object_attrs_meta.pop(C_DESC, None)

    def __get_property_id(self,p_property):
        """
        Получает id свойства объекта
        """
        l_id=None
        if not p_property:
            return None
        if type(p_property).__name__=="list": # если свойство объекта - лист
            l_id=[]
            for i_property in p_property:
                l_id.append(str(i_property.id))
        else:
            l_id=str(p_property.id)
        return l_id

    @property
    def metadata_json(self):
        """
        Метаданные объекта в формате json
        """
        l_json={}

        # основные атрибуты
        if self.name:
            l_json.update(
                {
                    C_NAME:self.name,
                }
            )
        if self.desc:
            l_json.update(
                {
                    C_DESC:self.desc,
                }
            )
        # атрибуты объекта Attribute
        if type(self).__name__=="Attribute":
            if self.attribute_type:
                l_json.update({C_TYPE_VALUE:self.attribute_type})
            if self.datatype:
                l_json.update(
                    {
                        C_DATATYPE:self.datatype.data_type_name,
                        C_LENGTH:self.datatype.data_type_length,
                        C_SCALE:self.datatype.data_type_scale
                    }
                )
            if self.type==C_ENTITY_COLUMN: # если атрибут сущности
                if self.pk:
                    l_json.update(
                        {
                            C_PK:self.pk
                        }
                    )
                if self.rk:
                    l_json.update(
                        {
                            C_RK:self.rk
                        }
                    )
                if self.fk:
                    l_json.update(
                        {
                            C_FK:self.fk
                        }
                    )


        # атрибуты queue таблицы
        if self.type==C_QUEUE:
            if self.schema:
                l_json.update({C_SCHEMA:self.schema})
            if self.name:
                 l_json.update({C_SOURCE_NAME:self.name})
            if self.queue_name:
                l_json.update({C_NAME:self.queue_name})
        # если указана связанная queue таблица
        if self.source_table:
            l_json.update(
                {
                    C_QUEUE:self.__get_property_id(p_property=self.source_table)
                }
            )
        # если указан атрибут источника
        if self.source_attribute:
            l_json.update(
                {
                    C_QUEUE_COLUMN:self.__get_property_id(p_property=self.source_attribute)
                }
            )
        # если указан источник
        if self.source:
            l_json.update(
                {
                    C_SOURCE:self.__get_property_id(p_property=self.source)
                }
            )
        # если указана сущность
        if self.entity:
            l_json.update(
                {
                    C_ENTITY:self.__get_property_id(p_property=self.entity)
                }
            )
        # если указаны натуральные ключи
        if self.type==C_IDMAP and self.source_attribute_nk:
            l_json.update(
                {
                    C_ATTRIBUTE_NK:self.__get_property_id(p_property=self.source_attribute_nk)
                }
            )
        # если заполнены атрибуты idmap
        if self.idmap_attribute:
            l_json.update(
                {
                    C_IDMAP_COLUMN:self.__get_property_id(p_property=self.idmap_attribute)
                }
            )

        if self.idmap:
            l_json.update(
                {
                    C_IDMAP:self.__get_property_id(p_property=self.idmap)
                }
            )

        # если заполнены атрибуты anchor
        if self.anchor_attribute:
            l_json.update(
                {
                    C_ANCHOR_COLUMN:self.__get_property_id(p_property=self.anchor_attribute)
                }
            )
        # если указан якорь
        if self.anchor:
            l_json.update(
                {
                    C_ANCHOR:self.__get_property_id(p_property=self.anchor)
                }
            )
        # Если указана таблица атрибут
        if self.attribute_table:
            l_json.update(
                {
                    C_ATTRIBUTE_TABLE:self.__get_property_id(p_property=self.attribute_table)
                }
            )
        # Если указаны атрибуты таблицы атрибут
        if self.attribute_table_attribute:
            l_json.update(
                {
                    C_ATTRIBUTE_COLUMN:self.__get_property_id(p_property=self.attribute_table_attribute)
                }
            )
        # Если указаны атрибуты сущности
        if self.entity_attribute:
            l_json.update(
                {
                    C_ENTITY_COLUMN:self.__get_property_id(p_property=self.entity_attribute)
                }
            )
        # Если указан tie
        if self.tie:
            l_json.update(
                {
                    C_TIE:self.__get_property_id(p_property=self.tie)
                }
            )
        # Если указаны атрибуты tie
        if self.tie_attribute:
            l_json.update(
                {
                    C_TIE_COLUMN:self.__get_property_id(p_property=self.tie_attribute)
                }
            )
        # Если указана связанная сущность
        if self.link_entity:
            l_json.update(
                {
                    C_LINK_ENTITY:self.__get_property_id(p_property=self.link_entity)
                }
            )
        # Если указана ошибка
        if type(self).__name__=="Job" or type(self).__name__=="Package":
            if self.error:
                l_json.update(
                    {
                        C_ERROR:self.error.replace("'","''") # экранирование одинарной кавычки
                    }
                )
            if self.start_datetime:
                l_json.update(
                    {
                        C_START_DATETIME:str(self.start_datetime)
                    }
                )
            if self.end_datetime:
                l_json.update(
                    {
                        C_END_DATETIME:str(self.end_datetime)
                    }
                )
        if type(self).__name__=="Package":
            if self.job:
                l_json.update(
                    {
                        C_ETL:self.__get_property_id(p_property=self.job)
                    }
                )
            if self.status:
                l_json.update(
                    {
                        C_STATUS:self.status
                    }
                )
        if type(self).__name__=="Job":
            if self.etl_id:
                l_json.update(
                    {
                        C_ETL_ATTRIBUTE_NAME:self.etl_id
                    }
                )
            if self.source_table_package:
                l_json.update(
                    {
                        C_QUEUE_ETL:self.__get_property_id(p_property=self.source_table_package)
                    }
                )
            if self.idmap_package:
                l_json.update(
                    {
                        C_IDMAP_ETL:self.__get_property_id(p_property=self.idmap_package)
                    }
                )
            if self.anchor_package:
                l_json.update(
                    {
                        C_ANCHOR_ETL:self.__get_property_id(p_property=self.anchor_package)
                    }
                )
            if self.attribute_table_package:
                l_json.update(
                    {
                        C_ATTRIBUTE_ETL:self.__get_property_id(p_property=self.attribute_table_package)
                    }
                )
            if self.tie_package:
                l_json.update(
                    {
                        C_TIE_ETL:self.__get_property_id(p_property=self.tie_package)
                    }
                )
        return l_json

    def metadata_object(self, p_id: str =None, p_attrs: dict =None) -> object:
        """
        Объект метаданных
        """
        return meta.MetaObject(
            p_type=self.type,
            p_uuid=str(p_id) if p_id else None,
            p_attrs=p_attrs

        )

    def create_metadata(self):
        """
        Записывает метаданные объекта
        """
        l_obj_meta=meta.search_object(p_uuid=[str(self.id)], p_type=self.type)
        if l_obj_meta.__len__()>0: # если объект уже существует в метаданных - обновляем его атрибуты
            meta.update_object(
                p_object=self.metadata_object(p_id=self.id, p_attrs=self.metadata_json)
            )
        else:
            meta.create_object(
                p_object=self.metadata_object(p_id=self.id, p_attrs=self.metadata_json)
            )

    def update_metadata(self):
        """
        Обновляет метаданные объекта
        """
        meta.update_object(
            p_object=self.metadata_object(p_id=self.id, p_attrs=self.metadata_json)
        )

    def delete_metadata(self):
        """
        Удаляет метаданные объекта
        """
        meta.delete_object(
            p_object=self.metadata_object(p_id=self.id)
        )



class Attribute(_DWHObject):
    """
    Атрибут
    """

    def __init__(self,
                 p_type: str,
                 p_id: str =None,
                 p_name: str =None,
                 p_datatype: str =None,
                 p_length: int =None,
                 p_scale: int =None,
                 p_attribute_type: str =None,
                 p_pk: int =None,
                 p_rk: int =None,
                 p_fk: int =None,
                 p_desc: str =None,
                 p_source_table =None,
                 p_source_attribute =None,
                 p_entity =None,
                 p_entity_attribute =None,
                 p_link_entity =None,
                 p_idmap =None,
                 p_anchor =None,
                 p_attribute_table =None,
                 p_tie =None
    ):
        super().__init__(
            p_id=p_id,
            p_type=p_type,
            p_desc=p_desc,
            p_source_table=p_source_table,
            p_source_attribute=p_source_attribute,
            p_entity=p_entity,
            p_entity_attribute=p_entity_attribute,
            p_link_entity=p_link_entity,
            p_idmap=p_idmap,
            p_anchor=p_anchor,
            p_attribute_table=p_attribute_table,
            p_tie=p_tie
        )
        self._name=p_name
        self._datatype=p_datatype
        self._length=p_length
        self._scale=p_scale
        self._attribute_type=p_attribute_type
        self._pk=p_pk
        self._rk=p_rk
        self._fk=p_fk


    @property
    def datatype(self):
        """
        Тип данных
        """
        l_datatype=self._datatype or self.object_attrs_meta.get(C_DATATYPE,None)
        l_length=self._length or self.object_attrs_meta.get(C_LENGTH,None)
        l_scale=self._scale or self.object_attrs_meta.get(C_SCALE,None)
        l_datatype_obj=None
        if l_datatype is not None:
            l_datatype_obj=Driver.DataType(
                p_dbms_type=Connection().dbms_type,
                p_data_type_name=l_datatype,
                p_data_type_length=l_length,
                p_data_type_scale=l_scale
            )
        return l_datatype_obj

    @property
    def attribute_type(self):
        """
        Тип атрибута
        """
        l_attribute_type=None
        if self._attribute_type is not None:
            l_attribute_type=self._attribute_type.lower()
            if l_attribute_type not in C_ATTRIBUTE_TABLE_TYPE_LIST:
                AbaseError(p_error_text="Incorrect type of attribute" + self._id, p_module="DWH",
                           p_class="Attribute", p_def="attribute_type").raise_error()
        return l_attribute_type or self.object_attrs_meta.get(C_TYPE_VALUE,None)

    @property
    def pk(self):
        """
        Признак ключа
        """
        if self._pk is not None:
            return self._pk
        else:
            return self.object_attrs_meta.get(C_PK,None)

    @property
    def rk(self):
        """
        Признак суррогата
        """
        if self._rk is not None:
            return self._rk
        else:
            return self.object_attrs_meta.get(C_RK,None)

    @property
    def fk(self):
        """
        Признак внешнего ключа
        """
        if self._fk is not None:
            return self._fk
        else:
            return self.object_attrs_meta.get(C_FK,None)

    def __source_attribute_exist_checker(self):
        """
        Проверяет наличие атрибута в таблице источнике
        """
        if self.source_table.schema+"."+self.source_table.name+"."+self.name not in self.source_table.source.source_attributes \
                and self.attribute_type not in ([C_UPDATE, C_ETL_ATTR]):
            AbaseError(p_error_text="Attribute " +self.source_table.schema+"."+self.source_table.name+"."+self.name + "wasn't found", p_module="DWH",
                       p_class="Attribute", p_def="__source_attribute_exist_checker").raise_error()


class Entity(_DWHObject):
    """
    Сущность
    """

    def __init__(self,
                 p_id: str =None,
                 p_name: str =None,
                 p_desc: str =None,
                 p_entity_attribute: list =None,
                 p_source: list =None,
                 p_source_table: list =None,
                 p_idmap: object =None,
                 p_anchor: object =None,
                 p_attribute_table: list =None,
                 p_tie: object =None
    ):
        """
        Конструктор

        :param p_id: id
        :param p_name:
        :param p_desc:
        """
        super().__init__(
            p_id=p_id,
            p_type=C_ENTITY,
            p_entity_attribute=p_entity_attribute,
            p_source=p_source,
            p_source_table=p_source_table,
            p_idmap=p_idmap,
            p_anchor=p_anchor,
            p_attribute_table=p_attribute_table,
            p_tie=p_tie,
            p_desc=p_desc,
            p_name=p_name
        )

    @property
    def name(self):
        """
        Наименование сущности
        """
        return super().name
    @name.setter
    def name(self, p_new_name: str):

        # изменяем наименование сущности
        self._name=p_new_name.lower()
        # изменяем наименования объектов путем изменения у них сущности
        # idmap
        l_idmap=self.idmap
        l_idmap.entity=self
        self.idmap=l_idmap
        #anchor
        l_anchor=self.anchor
        l_anchor.entity=self
        self.anchor=l_anchor
        #attribute
        l_attributes=[]
        for i_attribute in self.attribute_table:
            l_attribute=i_attribute
            l_attribute.entity=self
            l_attributes.append(l_attribute)
        self.attribute_table=l_attributes
        #tie
        l_ties=[]
        if self.tie:
            for i_tie in self.tie:
                l_tie=i_tie
                l_tie.entity=self
                l_ties.append(l_tie)
            self.tie=l_ties

    def get_entity_function(self):
        """
        Генерирует скрипт создания функции-конструктора запросов для сущности
        """
        # формируем словарь из атрибутов сущности и их типов данных
        l_entity_attr_dict={}
        for i_entity_attribute in self.entity_attribute:
            if i_entity_attribute.rk!=1: # не добавляем RK сущности, так как он и так добавляется всегда
                l_table=i_entity_attribute.tie or i_entity_attribute.attribute_table
                l_entity_attr_dict.update(
                    {
                        i_entity_attribute.name:{C_DATATYPE:i_entity_attribute.datatype.data_type_sql, C_TABLE:l_table.name}
                    }
                )
        return Connection().dbms.get_entity_function_sql(
            p_entity_name=self.name,
            p_entity_attribute_dict=l_entity_attr_dict
        )
    def get_drop_entity_function_sql(self):
        """
        Генерирует скрипт удаления функции-конструктора запросов
        """
        return Connection().dbms.get_drop_entity_function_sql(p_entity_name=self.name)


class SourceTable(_DWHObject):
    """
    Таблица источника
    """

    def __init__(self,
                 p_id: str =None,
                 p_name: str =None,
                 p_schema: str =None,
                 p_source_attribute: list =None,
                 p_source: object =None,
                 p_increment: object =None
    ):
        """
        Конструктор

        :param p_id:
        :param p_name:
        :param p_schema:
        :param p_attributes:
        """
        super().__init__(
            p_id=p_id,
            p_type=C_QUEUE,
            p_source_attribute=p_source_attribute,
            p_source=p_source
        )
        self._name=p_name
        self._schema=p_schema
        self._increment=p_increment

        # добавляем автоматически тенические атрибуты
        self.__create_source_attribute()

    @property
    def name(self):
        """
        Наименование таблицы источника
        """
        l_name=None
        if self._name is not None:
            l_name=self._name.lower()
        return l_name or self.object_attrs_meta.get(C_SOURCE_NAME,None)

    @name.setter
    def name(self, p_new_name: str):
        self._name=p_new_name


    @property
    def schema(self):
        """
        Наименование схемы таблицы источника
        """
        l_schema=None
        if self._schema is not None:
            l_schema=self._schema.lower()
        return l_schema or self.object_attrs_meta.get(C_SCHEMA,None)


    @property
    def queue_name(self):
        """
        Наименование таблицы queue
        """
        return self.source.name.replace(" ","_") +"_" + self.schema +"_" + self.name +"_" + C_QUEUE

    @property
    def increment(self):
        """
        Инкремент
        """
        if self._increment:
            if type(self._increment).__name__!="Attribute":
                AbaseError(p_error_text="p_increment doesn't belong to class Attribute",
                    p_module="DWH", p_class="SourceTable", p_def="increment").raise_error()
            # if self._increment.datatype.data_type_name!=C_TIMESTAMP_DBMS.get(self.source.type):
            #     sys.exit("У инкремента некорректный тип данных") #TODO переделать
        l_increment_meta_obj=self.object_attrs_meta.get(C_INCREMENT,None)
        l_increment=None
        if l_increment_meta_obj:
            l_increment=Attribute(
                p_id=l_increment_meta_obj,
                p_type=C_QUEUE_ATTR
            )
        return self._increment or l_increment


    @increment.setter
    def increment(self,p_new_increment: object):
        self._increment=p_new_increment

    @property
    def last_increment(self):
        """
        Возвращает значение последнего загруженного инкремента
        """
        l_increment=meta.search_object(
            p_type=C_QUEUE_INCREMENT,
            p_uuid=[self.id]
        )
        if l_increment: # если указано значение
            return l_increment
        elif not l_increment and self.increment: # если значения нет, но инкремент указан
            return "1900-01-01 00:00:00"
        else: # когда инкремент не указан
            return None

    @property
    def source_table_sql(self):
        """
        SQL-запрос захвата данных с источника
        """
        if self.source.type in [C_MYSQL]: # double quotes with name of database objects are unacceptable in these DBMS
            l_dbl_qts=''
        else:
            l_dbl_qts='"'
        l_attribute_sql=""
        l_attribute_name_list=[]
        for i_attribute in self.source_attribute:
            if i_attribute.attribute_type==C_QUEUE_ATTR: # не берем технические атрибуты
                l_attribute_name_list.append(i_attribute.name)
        l_attribute_name_list.sort() # сортируем по наименоваию
        for i_attribute in l_attribute_name_list:
            l_attribute_sql=l_attribute_sql+"\n"+l_dbl_qts+i_attribute+l_dbl_qts+","
        l_attribute_sql=l_attribute_sql[1:] # убираем первый перенос строки
        l_increment_sql=""
        l_timestamp_type=C_TIMESTAMP_DBMS.get(self.source.type)
        if self.increment:
            l_increment_attr_sql=l_dbl_qts+self.increment.name+l_dbl_qts+" AS update_timestamp"
        else:
            l_increment_attr_sql=self.source.current_timestamp_sql+" AS update_timestamp"
        if self.last_increment:
            l_increment_sql="\nWHERE "+l_dbl_qts+self.increment.name+l_dbl_qts+">CAST('"+self.last_increment+"' AS "+l_timestamp_type+")"
        l_sql="SELECT\n"+l_attribute_sql+"\n"+l_increment_attr_sql+"\nFROM "+l_dbl_qts+self.schema+l_dbl_qts+"."+l_dbl_qts+self.name+l_dbl_qts\
              +l_increment_sql+";"
        return l_sql

    def __create_source_attribute(self):
        """
        Создает технические атрибуты для source таблицы
        """
        update_column=Attribute(
            p_name=C_UPDATE_TIMESTAMP_NAME,
            p_type=C_QUEUE_COLUMN,
            p_datatype=C_TIMESTAMP_DBMS.get(Connection().dbms_type),
            p_attribute_type=C_UPDATE,
            p_source_table=self
        )
        etl_column=Attribute(
            p_name=C_ETL_ATTRIBUTE_NAME,
            p_type=C_QUEUE_COLUMN,
            p_datatype=C_BIGINT,
            p_attribute_type=C_ETL_ATTR,
            p_source_table=self
        )
        if not self.source_attribute:
            add_attribute(p_table=self, p_attribute=update_column)
            add_attribute(p_table=self, p_attribute=etl_column)
        else:
            l_attribute_type_list=[]
            for i_attribute in self.source_attribute:
                l_attribute_type_list.append(i_attribute.attribute_type)
            if C_UPDATE not in l_attribute_type_list:
                add_attribute(p_table=self, p_attribute=update_column)
            if C_ETL_ATTR not in l_attribute_type_list:
                add_attribute(p_table=self, p_attribute=etl_column)



class Idmap(_DWHObject):
    """
    Таблица Idmap
    """


    def __init__(self,
                 p_id: str =None,
                 p_idmap_attribute: list =None,
                 p_entity: object =None,
                 p_source_attribute_nk: list =None
    ):
        super().__init__(
            p_id=p_id,
            p_type=C_IDMAP,
            p_idmap_attribute=p_idmap_attribute,
            p_entity=p_entity
        )
        self._source_attribute_nk=p_source_attribute_nk

        # создаем автоматом атрибуты, если нет в метаданных
        self.__create_idmap_attribute()
        # добавляем/обновляем (!) idmap в сущность, если нет в метаданных
        self.__add_idmap_to_entity()

    @property
    def name(self):
        """
        Наименование idmap
        """
        if self._entity and self._entity.name:
            return str(self._entity.name).lower()+_get_table_postfix(p_table_type=C_IDMAP)
        else:
            return self.object_attrs_meta.get(C_NAME)

    @property
    def entity(self):
        """
        Сущность
        """
        return super().entity

    @entity.setter
    def entity(self, p_new_entity: object):
        self._entity=p_new_entity
        # изменяем наименование атрибутов
        l_idmap_atrribute=[]
        for i_attribute in self.idmap_attribute:
            l_attribute=i_attribute
            if l_attribute.attribute_type in (C_RK, C_NK):
                l_attribute.name=self.entity.name+"_"+i_attribute.attribute_type
            l_idmap_atrribute.append(l_attribute)
        self.idmap_attribute=l_idmap_atrribute
        # изменяем source_attribute_nk - TBD




    def __create_idmap_attribute(self):
        """
        Создает фиксированные атрибуты для idmap, если их нет в метаданных
        """
        if not self.idmap_attribute:
            rk_column=Attribute(
                p_name=self.entity.name +"_" + C_RK,
                p_type=C_IDMAP_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_RK,
                p_idmap=self
            )
            nk_column=Attribute(
                p_name=self.entity.name +"_" + C_NK,
                p_type=C_IDMAP_COLUMN,
                p_datatype=C_VARCHAR,
                p_length=4000,
                p_attribute_type=C_NK,
                p_idmap=self
            )
            etl_column=Attribute(
                p_name=C_ETL_ATTRIBUTE_NAME,
                p_type=C_IDMAP_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_ETL_ATTR,
                p_idmap=self
            )
            add_attribute(p_table=self, p_attribute=rk_column)
            add_attribute(p_table=self, p_attribute=nk_column)
            add_attribute(p_table=self, p_attribute=etl_column)

    def __add_idmap_to_entity(self):
        """
        Добавляет idmap в entity
        """
        self.entity.idmap=self

    @property
    def source_attribute_nk(self):
        """
        Атрибуты таблицы источника для составного ключа
        """
        l_attribute=[]
        l_attribute_meta_obj=self.object_attrs_meta.get(C_ATTRIBUTE_NK)
        if l_attribute_meta_obj:
            for i_attribute in l_attribute_meta_obj:
                l_attribute.append(
                    Attribute(
                        p_id=i_attribute,
                        p_type=C_QUEUE_COLUMN
                    )
            )
        return self._source_attribute_nk or l_attribute

    @source_attribute_nk.setter
    def source_attribute_nk(self, p_new_source_attribute_nk):
        self._source_attribute_nk=p_new_source_attribute_nk
        self.object_attrs_meta.pop(C_ATTRIBUTE_NK, None)






class Anchor(_DWHObject):
    """
    Якорь
    """
    def __init__(self,
                 p_id: str =None,
                 p_anchor_attribute: list =None,
                 p_entity: object =None
                 ):
        super().__init__(
            p_id=p_id,
            p_type=C_ANCHOR,
            p_anchor_attribute=p_anchor_attribute,
            p_entity=p_entity
        )

        # создаем автоматом атрибуты, если нет в метаданных
        self.__create_anchor_attribute()
        # добавляем/обновляем (!) anchor в сущность
        self.__add_anchor_to_entity()

    @property
    def name(self):
        """
        Наименование
        """
        if self._entity and self._entity.name:
           return str(self._entity.name).lower()+_get_table_postfix(p_table_type=C_ANCHOR)
        else:
           return self.object_attrs_meta.get(C_NAME)

    @property
    def entity(self):
        """
        Сущность
        """
        return super().entity

    @entity.setter
    def entity(self, p_new_entity: object):
        self._entity=p_new_entity
        # изменяем наименование атрибутов
        l_anchor_atrribute=[]
        for i_attribute in self.anchor_attribute:
            l_attribute=i_attribute
            if l_attribute.attribute_type in (C_RK):
                l_attribute.name=self.entity.name+"_"+i_attribute.attribute_type
            l_anchor_atrribute.append(l_attribute)
        self.anchor_attribute=l_anchor_atrribute

    def __create_anchor_attribute(self):
        """
        Генерирует атрибуты для anchor, если их нет в метаданных
        """
        if not self.anchor_attribute:
            rk_column=Attribute(
                p_name=self.entity.name +"_" + C_RK,
                p_type=C_ANCHOR_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_RK,
                p_anchor=self
            )
            source_system_id=Attribute(
                p_name=C_SOURCE_ATTRIBUTE_NAME,
                p_type=C_ANCHOR_COLUMN,
                p_datatype=C_INT,
                p_attribute_type=C_SOURCE_ATTR,
                p_anchor=self
            )
            etl_id=Attribute(
                p_name=C_ETL_ATTRIBUTE_NAME,
                p_type=C_ANCHOR_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_ETL_ATTR,
                p_anchor=self
            )
            add_attribute(p_table=self, p_attribute=rk_column)
            add_attribute(p_table=self, p_attribute=source_system_id)
            add_attribute(p_table=self, p_attribute=etl_id)

    def __add_anchor_to_entity(self):
        """
        Добавяет anchor в entity
        """
        self.entity.anchor=self


class AttributeTable(_DWHObject):
    """
    Таблица Атрибут
    """
    def __init__(self,
                 p_id: str =None,
                 p_attribute_table_attribute: list =None,
                 p_entity: object =None,
                 p_entity_attribute: object =None
                 ):
        super().__init__(
            p_id=p_id,
            p_type=C_ATTRIBUTE_TABLE,
            p_attribute_table_attribute=p_attribute_table_attribute,
            p_entity=p_entity,
            p_entity_attribute=p_entity_attribute
        )

        # создаем автоматом атрибуты, если нет в метаданных
        self.__create_attributetable_attribute()
        # добавляем таблицу атрибут в сущность
        self.__add_attribute_table_to_entity()

    @property
    def name(self):
        """
        Наименование
        """
        if self._entity_attribute and self._entity_attribute.name:
            return self._entity.name+"_"+self._entity_attribute.name\
                   +_get_table_postfix(p_table_type=C_ATTRIBUTE_TABLE)
        elif self._entity and self._entity.name:
            return self._entity.name+"_"+self.entity_attribute.name \
                   +_get_table_postfix(p_table_type=C_ATTRIBUTE_TABLE)
        else:
            return self.object_attrs_meta.get(C_NAME)
    @property
    def entity(self):
        """
        Сущность
        """
        return super().entity

    @entity.setter
    def entity(self, p_new_entity: object):
        self._entity=p_new_entity
        # изменяем наименование атрибутов
        l_atrribute_table_attr=[]
        for i_attribute in self.attribute_table_attribute:
            l_attribute=i_attribute
            if l_attribute.attribute_type==C_RK:
                l_attribute.name=self.entity.name+"_"+i_attribute.attribute_type
            l_atrribute_table_attr.append(l_attribute)
        self.attribute_table_attribute=l_atrribute_table_attr

    def __create_attributetable_attribute(self):
        """
         Генерирует атрибуты для attribute, если их нет в метаданных
        """
        if not self.attribute_table_attribute:
            rk_column=Attribute(
                p_name=self.entity.name +"_" + C_RK,
                p_type=C_ATTRIBUTE_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_RK,
                p_attribute_table=self
            )
            value_column=Attribute(
                p_name=self.entity_attribute.name,
                p_type=C_ATTRIBUTE_COLUMN,
                p_datatype=self.entity_attribute.datatype.data_type_name,
                p_length=self.entity_attribute.datatype.data_type_length,
                p_scale=self.entity_attribute.datatype.data_type_scale,
                p_attribute_type=C_VALUE,
                p_attribute_table=self
            )
            from_dttm=Attribute(
                p_name=C_FROM_ATTRIBUTE_NAME,
                p_type=C_ATTRIBUTE_COLUMN,
                p_datatype=C_TIMESTAMP,
                p_attribute_type=C_FROM,
                p_attribute_table=self
            )
            to_dttm=Attribute(
                p_name=C_TO_ATTRIBUTE_NAME,
                p_type=C_ATTRIBUTE_COLUMN,
                p_datatype=C_TIMESTAMP,
                p_attribute_type=C_TO,
                p_attribute_table=self
            )
            etl_id=Attribute(
                p_name=C_ETL_ATTRIBUTE_NAME,
                p_type=C_ATTRIBUTE_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_ETL_ATTR,
                p_attribute_table=self
            )
            add_attribute(p_table=self, p_attribute=rk_column)
            add_attribute(p_table=self, p_attribute=value_column)
            add_attribute(p_table=self, p_attribute=from_dttm)
            add_attribute(p_table=self, p_attribute=to_dttm)
            add_attribute(p_table=self, p_attribute=etl_id)

    def __add_attribute_table_to_entity(self):
        """
        Добавляет таблицу attribute в entity
        """

        l_attribute_id_list=self.entity.object_attrs_meta.get(C_ATTRIBUTE_TABLE)
        if not l_attribute_id_list:
            l_attribute_id_list=[]
        if self.id not in l_attribute_id_list:
            l_attribute=self.entity.attribute_table
            if l_attribute:
                l_attribute.append(self)
            else:
                l_attribute=[self]
            self.entity.attribute_table=l_attribute

class Tie(_DWHObject):
    """
    Связь
    """

    def __init__(self,
                 p_id: str =None,
                 p_tie_attribute: list =None,
                 p_entity: object =None,
                 p_link_entity =None,
                 p_entity_attribute =None,
                 p_source_table =None
                 ):
        super().__init__(
            p_id=p_id,
            p_type=C_TIE,
            p_tie_attribute=p_tie_attribute,
            p_link_entity=p_link_entity,
            p_entity_attribute=p_entity_attribute,
            p_source_table=p_source_table,
            p_entity=p_entity
        )

        # создаем автоматом атрибуты, если нет в метаданных
        self.__create_tie_attribute()
        # добавляем в сущности
        self.__add_tie_to_entity()

    @property
    def name(self):
        """
        Наименование
        """
        l_name=self.entity.name+"_"+\
               self.link_entity.name+\
               _get_table_postfix(p_table_type=C_TIE)
        return l_name

    @property
    def entity(self):
        """
        Сущность
        """
        return super().entity

    @entity.setter
    def entity(self, p_new_entity: object):
        self._entity=p_new_entity
        # изменяем наименование атрибутов
        l_tie_attribute=[]
        for i_attribute in self.tie_attribute:
            l_attribute=i_attribute
            if l_attribute.attribute_type==C_RK:
                l_attribute.name=self.entity.name+"_"+i_attribute.attribute_type
            l_tie_attribute.append(l_attribute)
        self.tie_attribute=l_tie_attribute

    @property
    def link_entity(self):
        """
        Связанная сущность
        """
        return super().link_entity

    @link_entity.setter
    def link_entity(self, p_new_link_entity):
        self._link_entity=p_new_link_entity
        # изменяем наименование атрибутов
        l_tie_attribute=[]
        for i_attribute in self.tie_attribute:
            l_attribute=i_attribute
            if l_attribute.attribute_type==C_LINK_RK:
                l_attribute.name=self.link_entity.name+"_"+C_RK
            l_tie_attribute.append(l_attribute)
        self.tie_attribute=l_tie_attribute


    def __create_tie_attribute(self):
        """
        Генерирует атрибуты для tie, если их нет в метаданных
        """
        if not self.tie_attribute:
            rk_column=Attribute( # атрибутом типом rk становится первый по порядку
                p_name=self.entity.name +"_" + C_RK,
                p_type=C_TIE_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_RK,
                p_tie=self
            )
            link_rk_column=Attribute( # атрибутом типом link_rk становится второй по порядку
                p_name=self.link_entity.name +"_" + C_RK,
                p_type=C_TIE_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_LINK_RK,
                p_tie=self
            )
            from_dttm=Attribute(
                p_name=C_FROM_ATTRIBUTE_NAME,
                p_type=C_TIE_COLUMN,
                p_datatype=C_TIMESTAMP,
                p_attribute_type=C_FROM,
                p_tie=self
            )
            to_dttm=Attribute(
                p_name=C_TO_ATTRIBUTE_NAME,
                p_type=C_TIE_COLUMN,
                p_datatype=C_TIMESTAMP,
                p_attribute_type=C_TO,
                p_tie=self
            )
            etl_id=Attribute(
                p_name=C_ETL_ATTRIBUTE_NAME,
                p_type=C_TIE_COLUMN,
                p_datatype=C_BIGINT,
                p_attribute_type=C_ETL_ATTR,
                p_tie=self
            )
            add_attribute(p_table=self, p_attribute=rk_column)
            add_attribute(p_table=self, p_attribute=link_rk_column)
            add_attribute(p_table=self, p_attribute=from_dttm)
            add_attribute(p_table=self, p_attribute=to_dttm)
            add_attribute(p_table=self, p_attribute=etl_id)

    def __add_tie_to_entity(self):
        """
        Добавляет tie в сущности
        """

        l_tie_id_list=self.entity.object_attrs_meta.get(C_TIE)
        if not l_tie_id_list:
            l_tie_id_list=[]
        if self.id not in l_tie_id_list:
            l_entity_tie=self.entity.tie
            if l_entity_tie:
                l_entity_tie.append(self)
            else:
                l_entity_tie=[self]
            self.entity.tie=l_entity_tie

        l_link_tie_id_list=self.link_entity.object_attrs_meta.get(C_TIE)
        if not l_link_tie_id_list:
            l_link_tie_id_list=[]
        if self.id not in l_link_tie_id_list:
            l_link_entity_tie=self.link_entity.tie
            if l_link_entity_tie:
                l_link_entity_tie.append(self)
            else:
                l_link_entity_tie=[self]
            self.link_entity.tie=l_link_entity_tie


class Job(_DWHObject):
    """
    Job по загрузке данных
    """
    def __init__(self,
                 p_id: str =None,
                 p_entity: object =None,
                 p_entity_attribute: object =None,
                 p_type: str =None,
                 p_source_table: object =None,
                 p_anchor: object =None,
                 p_attribute_table: object =None,
                 p_tie: object =None,
                 p_idmap: object =None
    ):
        """
        Конструктор
        :param p_id: id job-a
        :param p_entity: список сущностей, в которые требуется загрузить данные
        :param p_entity_attribute: список атрибутов сущностей, в которые требуется загрузить данные
        :param p_status: статус джоба
        :param p_package: пакеты джоба
        :param p_type: тип объекта (заполняется при иницилизации объекта класса Package (дочерний класс), для Job - пусто)
        """
        super().__init__(
            p_id=p_id,
            p_entity=p_entity,
            p_type=p_type or C_ETL,
            p_entity_attribute=p_entity_attribute,
            p_source_table=p_source_table,
            p_idmap=p_idmap,
            p_anchor=p_anchor,
            p_attribute_table=p_attribute_table,
            p_tie=p_tie
        )

        self._start_datetime=copy.copy(datetime.datetime.now()) # чтобы время не изменялось при каждом вызове
        self._end_datetime=None
        self._source_table_package=None
        self._idmap_package=None
        self._anchor_package=None
        self._attribute_table_package=None
        self._tie_package=None
        self._error=None

    @property
    def etl_id(self) -> int:
        """
        Целочисленный id для хранения в ХД
        """
        # вычисляем максимальный etl_id из метаданных
        l_max_etl_id=get_max_etl_id()
        l_first_etl_id=None
        if l_max_etl_id==0:
            l_first_etl_id=1
        return self.object_attrs_meta.get(C_ETL_ATTRIBUTE_NAME,None) \
               or l_first_etl_id \
               or l_max_etl_id+1

    @property
    def start_datetime(self) -> datetime:
        """
        Дата и время начала job-а
        """
        l_start_datetime=str(self.object_attrs_meta.get(C_START_DATETIME,None) or self._start_datetime)
        return datetime.datetime.strptime(l_start_datetime,'%Y-%m-%d %H:%M:%S.%f')
    @property
    def end_datetime(self) -> datetime:
        """
        Дата и время окончания джоба
        """
        l_end_datetime=str(self.object_attrs_meta.get(C_END_DATETIME,None) or self._end_datetime)
        return datetime.datetime.strptime(l_end_datetime,'%Y-%m-%d %H:%M:%S.%f')

    @end_datetime.setter
    def end_datetime(self, p_new_end_datetime: str):
        self._end_datetime=p_new_end_datetime

    @property
    def duration(self):
        """
        Длительность job-а в микросекундах
        """
        l_dur=self.end_datetime-self.start_datetime
        return l_dur.microseconds

    @property
    def status(self):
        """
        Статус job-а
        """
        l_fail=None
        # если хотя бы один объект прогружен с ошибкой - статус джоба ошибка
        for i_src_pack in self.source_table_package:
            if i_src_pack.status==C_STATUS_FAIL:
                l_fail=C_STATUS_FAIL
        for i_idmp_pack in self.idmap_package:
            if i_idmp_pack.status==C_STATUS_FAIL:
                l_fail=C_STATUS_FAIL
        for i_anchr_pack in self.anchor_package:
            if i_anchr_pack.status==C_STATUS_FAIL:
                l_fail=C_STATUS_FAIL
        for i_attr_pack in self.attribute_table_package:
            if i_attr_pack.status==C_STATUS_FAIL:
                l_fail=C_STATUS_FAIL
        if self.tie_package: # так как может не быть tie у джоба
            for i_tie_pack in self.tie_package:
                if i_tie_pack.status==C_STATUS_FAIL:
                    l_fail=C_STATUS_FAIL
        return l_fail or C_STATUS_SUCCESS

    @property
    def error(self):
        """
        Текст ошибки
        """
        return self.object_attrs_meta.get(C_ERROR,None) or self._error

    @error.setter
    def error(self, p_new_error: str):
        self._error=p_new_error

    @property
    def source_table_package(self):
        """
        ETL-пакет таблиц источника
        """
        # проверка объекта
        _class_checker(p_class_name="Package",p_object=self._source_table_package)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_QUEUE_ETL,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Package", p_type=C_QUEUE_ETL)

        return self._source_table_package or l_object

    @property
    def idmap_package(self):
        """
        ETL-пакеты idmap
        """
        # проверка объекта
        _class_checker(p_class_name="Package",p_object=self._idmap_package)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_IDMAP_ETL,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Package", p_type=C_IDMAP_ETL)

        return self._idmap_package or l_object

    @property
    def anchor_package(self):
        """
        ETL-пакеты idmap
        """
        # проверка объекта
        _class_checker(p_class_name="Package",p_object=self._anchor_package)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ANCHOR_ETL,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Package", p_type=C_ANCHOR_ETL)

        return self._anchor_package or l_object

    @property
    def attribute_table_package(self):
        """
        ETL-пакеты idmap
        """
        # проверка объекта
        _class_checker(p_class_name="Package",p_object=self._attribute_table_package)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ATTRIBUTE_ETL,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Package", p_type=C_ATTRIBUTE_ETL)

        return self._attribute_table_package or l_object

    @property
    def tie_package(self):
        """
        ETL-пакеты idmap
        """
        # проверка объекта
        _class_checker(p_class_name="Package",p_object=self._tie_package)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_TIE_ETL,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Package", p_type=C_TIE_ETL)

        return self._tie_package or l_object

    @source_table_package.setter
    def source_table_package(self, p_new):
        self._source_table_package=p_new

    @idmap_package.setter
    def idmap_package(self, p_new):
        self._idmap_package=p_new

    @anchor_package.setter
    def anchor_package(self, p_new):
        self._anchor_package=p_new

    @attribute_table_package.setter
    def attribute_table_package(self, p_new):
        self._attribute_table_package=p_new

    @tie_package.setter
    def tie_package(self, p_new):
        self._tie_package=p_new

    def start_job(self, p_step_name: str =C_STG_SCHEMA):
        """
        Загружает данные в ХД

        :param p_step_name: наименование схемы данных, с которой начинается прогрузка данных (по умолчанию stg)
        """
        # получаем все таблицы, которые требуется прогрузить
        l_source_table_list=[]
        l_idmap_list=[]
        l_anchor_list=[]
        l_attribute_list=[]
        l_tie_list=[]
        if self.entity or self.entity_attribute:
            if self.entity_attribute: # если указан атрибут грузим только его (даже если указана и таблица)
                # таблицы источники
                for i_source_attribute in self.entity_attribute.source_attribute:
                    l_source_table_list.append(i_source_attribute.source_table)
                # idmap
                l_idmap_list.append(self.entity_attribute.entity.idmap)
                # anchor
                l_anchor_list.append(self.entity_attribute.entity.anchor)
                # attribute
                l_attribute_meta=meta.search_object( # поиск таблицы атрибута в метаданных
                    p_type=C_ATTRIBUTE_TABLE,
                    p_attrs={C_ENTITY_COLUMN:self.entity_attribute.id}
                )[0]
                l_attribute=AttributeTable(p_id=l_attribute_meta.uuid)
                l_attribute_list.append(l_attribute)
                # tie - не обновляем, если указан атрибут сущности!
            else:
                # таблицы источники
                for i_source_table in self.entity.source_table:
                    l_source_table_list.append(i_source_table)
                # idmap
                l_idmap_list.append(self.entity.idmap)
                # anchor
                l_anchor_list.append(self.entity.anchor)
                # attribute
                l_attribute_list.extend(self.entity.attribute_table)
                # tie
                if self.entity.tie:
                    l_tie_list.extend(self.entity.tie)
        else:
            # таблицы источники
            l_source_table_meta=meta.search_object(p_type=C_QUEUE)
            for i_source_table_meta in l_source_table_meta:
                 l_source_table=SourceTable(p_id=i_source_table_meta.uuid)
                 l_entity_meta=meta.search_object(p_type=C_ENTITY)
                 if l_entity_meta:
                     for i_ent in l_entity_meta:
                         if l_source_table.id in i_ent.attrs.get(C_QUEUE,None) and l_source_table not in l_source_table_list: # грузим только те таблицы источники, которые используются в сущностях
                            l_source_table_list.append(l_source_table)
            # idmap
            l_idmap_table_meta=meta.search_object(p_type=C_IDMAP)
            for i_idmap_meta in l_idmap_table_meta:
                l_idmap=Idmap(p_id=i_idmap_meta.uuid)
                l_idmap_list.append(l_idmap)
            # anchor
            l_anchor_meta=meta.search_object(p_type=C_ANCHOR)
            for i_anchor_meta in l_anchor_meta:
                l_anchor=Anchor(p_id=i_anchor_meta.uuid)
                l_anchor_list.append(l_anchor)
            # attribute
            l_attribute_meta=meta.search_object(p_type=C_ATTRIBUTE_TABLE)
            for i_attribute_meta in l_attribute_meta:
                l_attribute=AttributeTable(p_id=i_attribute_meta.uuid)
                l_attribute_list.append(l_attribute)
            # tie
            l_tie_meta=meta.search_object(p_type=C_TIE)
            for i_tie_meta in l_tie_meta:
                l_tie=Tie(p_id=i_tie_meta.uuid)
                l_tie_list.append(l_tie)


        # грузим данные в таблицы источники
        if p_step_name==C_STG_SCHEMA:
            print(C_COLOR_BOLD+"==============================")
            print("Source tables loading")
            print("=============================="+C_COLOR_ENDC)
            self.source_table_load(p_source_table=l_source_table_list)
        # грузим данные в idmap
        if p_step_name in [
            C_IDMAP_SCHEMA,
            C_STG_SCHEMA
        ]:
            print(C_COLOR_BOLD+"==============================")
            print("Idmap tables loading")
            print("=============================="+C_COLOR_ENDC)
            self.idmap_load(p_idmap=l_idmap_list)
        if p_step_name in [
            C_IDMAP_SCHEMA,
            C_STG_SCHEMA,
            C_AM_SCHEMA
        ]:
            print(C_COLOR_BOLD+"==============================")
            print("Anchor tables loading")
            print("=============================="+C_COLOR_ENDC)
            self.anchor_load(p_anchor=l_anchor_list)
            print(C_COLOR_BOLD+"==============================")
            print("Attribute tables loading")
            print("=============================="+C_COLOR_ENDC)
            self.attribute_table_load(p_attribute_table=l_attribute_list)
            if l_tie_list.__len__()>0: # не всегда ест tie у сущности
                print(C_COLOR_BOLD+"==============================")
                print("Tie tables loading")
                print("=============================="+C_COLOR_ENDC)
                self.tie_load(p_tie=l_tie_list)
        # записываем метаданные
        self.end_datetime=datetime.datetime.now() # проставляем дату окончания процесса
        # записываем в метаданные
        self.create_metadata()



    def __status_color(self, p_status: str):
        """
        Определяет цвет статуса в терминале
        """
        if p_status==C_STATUS_SUCCESS:
            return C_COLOR_OKGREEN+p_status+C_COLOR_ENDC
        if p_status==C_STATUS_PARTLY_SUCCESS:
            return C_COLOR_WARNING+p_status+C_COLOR_ENDC
        if p_status==C_STATUS_FAIL:
            return C_COLOR_FAIL+p_status+C_COLOR_ENDC
        if p_status==C_STATUS_IN_PROGRESS:
            return C_COLOR_OKBLUE+p_status+C_COLOR_ENDC


    def source_table_load(self, p_source_table: list):
        """
        Загружает данные в таблицы источники

        :param p_source_table: список таблиц источников, которые требуется прогрузить
        """
        for i_source_table in p_source_table:
            print("("+i_source_table.source.name+") "+i_source_table.name+" : "+self.__status_color(C_STATUS_IN_PROGRESS), end="")
            l_package=Package(
                p_source_table=i_source_table,
                p_type=C_QUEUE_ETL,
                p_job=self
            )
            l_package.start_etl()
            print("\r"+"("+i_source_table.source.name+") "+i_source_table.name+" : "+self.__status_color(l_package.status))

    def idmap_load(self, p_idmap: list):
        """
        Загружает данные в idmap таблицы

        :param p_idmap: список idmap, которые требуется прогрузить
        """
        for i_idmap in p_idmap:
            print(i_idmap.name+" : "+self.__status_color(C_STATUS_IN_PROGRESS), end="")
            l_error_cnt=0
            l_source_table_cnt=0
            for i_source_table in i_idmap.entity.source_table:
                l_package=Package(
                    p_idmap=i_idmap,
                    p_type=C_IDMAP_ETL,
                    p_job=self,
                    p_source_table=i_source_table
                )
                l_package.start_etl()
                if l_package.error:
                    l_error_cnt+=1
                l_source_table_cnt+=1
            if l_error_cnt==l_source_table_cnt:
                l_status=C_STATUS_FAIL
            elif l_error_cnt==0:
                l_status=C_STATUS_SUCCESS
            else:
                l_status=C_STATUS_PARTLY_SUCCESS
            print("\r"+i_idmap.name+" : "+self.__status_color(l_status))

    def anchor_load(self, p_anchor: list):
        """
        Загружает данные в якоря

        :param p_anchor: список якорей, которые требуется прогрузить
        """
        for i_anchor in p_anchor:
            print(i_anchor.name+" : "+self.__status_color(C_STATUS_IN_PROGRESS), end="")
            l_package=Package(
                p_anchor=i_anchor,
                p_type=C_ANCHOR_ETL,
                p_job=self
            )
            l_package.start_etl()
            print("\r"+i_anchor.name+" : "+self.__status_color(l_package.status))

    def attribute_table_load(self, p_attribute_table: list):
        """
        Загружает данные в таблицы атрибуты

        :param p_attribute_table: список таблиц атрибутов, которые требуется прогрузить
        """
        for i_attribute_table in p_attribute_table:
            print(i_attribute_table.name+" : "+self.__status_color(C_STATUS_IN_PROGRESS), end="")
            l_error_cnt=0
            l_source_table_cnt=0
            for i_source_attribute in i_attribute_table.entity_attribute.source_attribute:
                l_package=Package(
                    p_attribute_table=i_attribute_table,
                    p_source_table=i_source_attribute.source_table,
                    p_job=self,
                    p_type=C_ATTRIBUTE_ETL
                )
                l_package.start_etl()
                if l_package.error:
                    l_error_cnt+=1
                l_source_table_cnt+=1
            if l_error_cnt==l_source_table_cnt:
                l_status=C_STATUS_FAIL
            elif l_error_cnt==0:
                l_status=C_STATUS_SUCCESS
            else:
                l_status=C_STATUS_PARTLY_SUCCESS
            print("\r"+i_attribute_table.name+" : "+self.__status_color(l_status))

    def tie_load(self, p_tie: list):
        """
        Загружает данные в tie

        :param p_tie: список таблиц tie, которые требуется прогрузить
        """
        for i_tie in p_tie:
            print(i_tie.name+" : "+self.__status_color(C_STATUS_IN_PROGRESS), end="")
            l_error_cnt=0
            l_source_table_cnt=0
            for i_source_table in i_tie.source_table:
                l_package=Package(
                    p_tie=i_tie,
                    p_source_table=i_source_table,
                    p_job=self,
                    p_type=C_TIE_ETL
                )
                l_package.start_etl()
                if l_package.error:
                    l_error_cnt+=1
                l_source_table_cnt+=1
            if l_error_cnt==l_source_table_cnt:
                l_status=C_STATUS_FAIL
            elif l_error_cnt==0:
                l_status=C_STATUS_SUCCESS
            else:
                l_status=C_STATUS_PARTLY_SUCCESS
            print("\r"+i_tie.name+" : "+self.__status_color(l_status))





class Package(Job):
    """
    ETL-пакет
    """

    def __init__(self,
                 p_type: str,
                 p_id: str =None,
                 p_source_table: object =None,
                 p_anchor: object =None,
                 p_attribute_table: object =None,
                 p_tie: object =None,
                 p_idmap: object =None,
                 p_job: object =None
    ):
        """
        Конструктор

        :param p_id: id etl-пакета
        :param p_type: тип etl-пакета
        :param p_source_table: таблица источник
        :param p_anchor_table: якорь
        :param p_attribute_table: таблица атрибут
        :param p_tie: таблица связи
        :param p_idmap: idmap
        :param p_job: job, в рамках которого запустился пакет
        """
        super().__init__(
            p_type=p_type,
            p_id=p_id,
            p_source_table=p_source_table,
            p_idmap=p_idmap,
            p_anchor=p_anchor,
            p_attribute_table=p_attribute_table,
            p_tie=p_tie
        )

        self._job=p_job
        self._status=None

        # добавляем пакет к джобу при инициализации объекта
        self.__add_package_to_job()

    @property
    def status(self) -> str:
        """
        Статус etl-процесса
        """
        return self.object_attrs_meta.get(C_STATUS) or self._status

    @status.setter
    def status(self, p_new_status: str):
        self._status=p_new_status

    @property
    def job(self):
        """
        Job
        """
        # проверка объекта
        _class_checker(p_class_name="Job",p_object=self._job)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(C_ETL,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Job")

        return self._job or l_object

    def start_etl(self, p_attribute_value: list =None):
        """
        Запускает etl таблицы

        :param p_attribute_value: значения для вставки (при загрузке таблиц источников)
        """
        # получаем etl в зависимости от типа объекта
        l_etl=None
        l_result=None
        # загружаем данные
        # таблицы источники
        if self.type==C_QUEUE_ETL:
            l_attribute_value=self.get_source_data() # получаем данные с источника, преобразованные для вставки в таблицу
            if l_attribute_value[1]: # если захват данных с источника завершился ошибкой
                l_result=None, l_attribute_value[1]
            else:
                # формируем sql-запрос
                l_etl=get_source_table_etl(p_source_table=self.source_table, p_attribute_value=l_attribute_value[0])
        # idmap
        elif self.type==C_IDMAP_ETL:
            # sql-запрос
            l_etl=get_idmap_etl(p_idmap=self.idmap, p_etl_id=str(self.etl_id), p_source_table=self.source_table)[0]
        # anchor
        elif self.type==C_ANCHOR_ETL:
            # sql-запрос
            l_etl=get_anchor_etl(p_anchor=self.anchor, p_etl_id=str(self.etl_id))
        # attribute
        elif self.type==C_ATTRIBUTE_ETL:
            # sql-запрос
            l_etl=get_attribute_etl(p_attribute_table=self.attribute_table, p_source_table=self.source_table, p_etl_id=str(self.etl_id))[0]
        #tie
        elif self.type==C_TIE_ETL:
            #sql-запрос
            l_etl=get_tie_etl(p_tie=self.tie, p_source_table=self.source_table, p_etl_id=str(self.etl_id))[0]
        # выполняем запрос в ХД
        if not l_result:
            l_result=Connection().sql_exec(p_sql=l_etl, p_result=0)
        # логируем
        self.end_datetime=datetime.datetime.now() # проставляем дату окончания процесса
        if l_result[1]: # если завершилось с ошибкой
            self.status=C_STATUS_FAIL
            # self.error=getattr(l_result[1], "pgerror",None) or l_result[1] # бывают случаи, когда pgerror не указан
            self.error=l_result[1]
        else:
            self.status=C_STATUS_SUCCESS
        # записываем в метаданные
        self.create_metadata()
        return l_etl

    def get_source_data(self):
        """
        Получает данные с источника и преобразует их для вставки в queue
        """
        # захват данных с источника
        l_data_frame=self.source_table.source.sql_exec(
            p_sql=self.source_table.source_table_sql
        )
        # формирование словаря с cast() выражением для каждого атрибута
        l_cast_dict={}
        l_attribute_name_list=[] # список с наименованиями атрибутов для последующей сортировки
        for i_attribute in self.source_table.source_attribute:
            if i_attribute.attribute_type==C_QUEUE_ATTR:
                l_attribute_name_list.append(i_attribute.name)
        l_attribute_name_list.sort()
        i=0
        for i_attribute_name in l_attribute_name_list:
            for i_attribute in self.source_table.source_attribute:
                if i_attribute.name==i_attribute_name:
                    l_cast_dict.update(
                        {i:"CAST("+str(i)+"AS "+i_attribute.datatype.data_type_sql+")"}
                    )
                    i+=1
        l_cast_dict.update(
            {
                i:"CAST("+str(i)+"AS "+C_TIMESTAMP_DBMS.get(Connection().dbms_type)+")"
            }
        )
        if l_data_frame[1]: # если возникла ошибка
            return None, l_data_frame[1]
        else:
            l_insert_data_frame=get_values_sql(p_data_frame=l_data_frame[0], p_cast_dict=l_cast_dict, p_etl_id=self.etl_id) # data frame для вставки в таблицу

        return l_insert_data_frame, None

    def __add_package_to_job(self):
        """
        Добавляет пакет к джобу
        """
        if self._job: # только если джоб указан при инициализации объекта
            add_attribute(p_table=self.job, p_attribute=self, p_add_table_flg=0)






