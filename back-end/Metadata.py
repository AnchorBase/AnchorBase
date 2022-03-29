# coding=utf-8
import Postgresql as pgsql
import json
from Constants import *
import metadata_config
import uuid
import copy
from SystemObjects import *


def sql_exec(p_sql: str, p_result: int =1):
    """
    Выполняет запросы к метаданным
    :param p_sql: SQL-запрос
    """
    l_result=pgsql.sql_exec(
        p_database=metadata_config.database,
        p_server=metadata_config.server,
        p_user=metadata_config.user,
        p_password=metadata_config.password,
        p_port=metadata_config.port,
        p_sql=p_sql,
        p_result=p_result
    )
    if l_result[1]:
        AbaseError(p_error_text="DBMS Error: "+l_result[1], p_module="Metadata", p_class="", p_def="sql_exec").raise_error()    ##if error - raise error
    else:
        return l_result[0]

def __create_ddl_metadata():
    """
    Генерирует скрипт создания таблиц метаданных

    """

    l_sql=pgsql.C_UUID_EXTENSION+"\n"
    l_sql+="DROP SCHEMA IF EXISTS "+'"'+C_META_SCHEMA+'" CASCADE;\nCREATE SCHEMA '+'"'+C_META_SCHEMA+'";\n'
    for i in C_META_TABLES:
        l_sql+='CREATE TABLE '+'"'+C_META_SCHEMA+'"."'+i+'"'+'(\n \tid '+C_UUID+' PRIMARY KEY,\n \tvalue '+ C_JSON+' NOT NULL\n);\n'
    return l_sql

def create_meta_tables():
    """
    Создает таблицы метаданных
    """
    l_sql=__create_ddl_metadata()
    sql_exec(p_sql=l_sql, p_result=0)


def __search_uuid_sql(p_uuid_list: list):
    """
    Формирует запрос поиска по метаданным с помощью uuid

    :param p_uuid_list: список uuid объекта/объектов метаданных
    """
    l_sql=" AND id IN ("
    for i_uuid in p_uuid_list:
        uuid.UUID(i_uuid) # проверка на корректный uuid
        l_sql=l_sql+"'"+str(i_uuid)+"'"+","
    return l_sql[:-1]+")"

def __search_attr_sql(p_attr_dict: dict):
    """
    Формирует SQL запрос поиска по метаданным с помощью параметров атрибута

    :param p_attr_dict: Словарь параметров атрибута
    """
    l_sql=""
    l_attr_key_list=list(p_attr_dict.keys())
    for i_attr_key in l_attr_key_list:
        l_sql=l_sql+" AND value ->> '"+str(i_attr_key)+"'='"+str(p_attr_dict.get(i_attr_key,None))+"'"
    return l_sql

def __insert_object_sql(p_type: str, p_uuid: str, p_attrs: dict):
    """
    Формирует запрос вставки метаданных

    :param p_type: тип объекта метаданных
    :param p_uuid: uuid объекта метаданных
    :param p_attrs: атрибуты объекта метаданных
    """
    l_attrs=json.dumps(p_attrs) # преобразуем в json строку
    l_sql='INSERT INTO "'+C_META_SCHEMA+'"."'+p_type+'"'+" (id, value) VALUES \n('"+str(p_uuid)+"','"+l_attrs+"');"
    return l_sql

def __update_object_sql(p_type: str, p_uuid: str, p_attrs: dict):
    """
    Формирует запрос изменения данных
    :param p_type: тип объекта метаданных
    :param p_uuid: uuid объекта метаданных
    :param p_attrs: атрибуты объекта метаданных
    """
    l_attrs=json.dumps(p_attrs) # преобразуем в json строку
    l_sql='UPDATE "'+C_META_SCHEMA+'"."'+p_type+'"'+"\nSET value='"+l_attrs+"'\nWHERE id='"+str(p_uuid)+"';"
    return l_sql

def __delete_object_sql(p_type: str, p_uuid: str):
    """
    Формирует запрос удаления из метаданных

    :param p_type: тип объекта метаданных
    :param p_uuid: uuid объекта
    """
    l_sql='DELETE FROM "'+C_META_SCHEMA+'"."'+p_type+'"'+" WHERE ID='"+str(p_uuid)+"';"
    return l_sql



def search_object(p_type: str, p_uuid: list =None, p_attrs: dict =None) -> list:
    """
    Поиск объекта/объектов в метаданных
    :param p_type: тип объекта метаданных
    :param p_uuid: uuid объекта метаданных
    :param p_attrs: атрибуты объекта метаданных
    """
    # проверка заданного типа объекта метаданных
    l_type=p_type.lower()
    if l_type not in C_META_TABLES:
        AbaseError(p_error_text="There's no object "+l_type+" in metadata", p_module="Metadata", p_class="", p_def="search_object").raise_error()   ##Raises error if there's no object of metadata
    # фомируем SELECT
    l_sql='SELECT * FROM "'+C_META_SCHEMA+'"."'+l_type+'"'+" WHERE 1=1"
    # добавляем условия фильтрации
    l_where=""
    # фильтрация по id
    if p_uuid is not None and p_uuid.count(None)==0:
        l_where = l_where + __search_uuid_sql(p_uuid)
    elif p_uuid is not None and p_uuid.count(None)>0:
        AbaseError(p_error_text="Empty value uuid", p_module="Metadata", p_class="", p_def="search_object").raise_error()
        # filtering by attr фильтрация по attr
    if p_attrs is not None:
        l_where= l_where + __search_attr_sql(p_attrs)
    l_sql=l_sql+l_where+";" # финальный SQL-запрос к метаданным
    l_result=sql_exec(l_sql) # выполняем запрос в БД метаданных
    if l_result.__len__()==0:
        return l_result
    l_objects=[]
    for i_obj in l_result:
        l_obj=MetaObject(
            p_uuid=i_obj[0],
            p_type=p_type,
            p_attrs=i_obj[1]
        )
        l_objects.append(l_obj)
    return l_objects

def create_object(p_object: object):
    """
    Запись в метаданные объекта

    :param p_object: объект метаданных (объект класса MetaObject)
    """
    # проверка, что передаваемый объект - объект класса MetaObject
    if type(p_object).__name__!="MetaObject":
        AbaseError(p_error_text="Object doesn't belong to MetaObject class", p_module="Metadata", p_class="", p_def="create_object").raise_error()
    # формируем SQL-запрос
    l_sql=__insert_object_sql(
        p_type=p_object.type,
        p_uuid=p_object.uuid,
        p_attrs=p_object.attrs
    )
    sql_exec(p_sql=l_sql, p_result=0) # выполняем sql-запрос

def update_object(p_object: object):
    """
    Обновление метаданных объекта.
    Все атрибуты в метаданных будут заменены на атрибуты передаваемого объекта

    :param p_object: объект класса MetaObject
    """
    if type(p_object).__name__!="MetaObject":
        AbaseError(p_error_text="Object doesn't belong to MetaObject class", p_module="Metadata", p_class="", p_def="update_object").raise_error()
    # формируем SQL-запрос
    l_sql=__update_object_sql(
        p_type=p_object.type,
        p_uuid=p_object.uuid,
        p_attrs=p_object.attrs
    )
    # выполняем запрос
    sql_exec(
        p_sql=l_sql,
        p_result=0
    )

def delete_object(p_object: object):
    """
    Удаляет метаданные объекта

    :param p_object: объект метаданных
    """
    if type(p_object).__name__!="MetaObject":
        AbaseError(p_error_text="Object doesn't belong to MetaObject class", p_module="Metadata", p_class="", p_def="delete_object").raise_error()
    # формируем SQL-запрос
    l_sql=__delete_object_sql(
        p_type=p_object.type,
        p_uuid=p_object.uuid
    )
    # выполняем запрос
    sql_exec(
        p_sql=l_sql,
        p_result=0
    )




class MetaObject:
    """
    Объект метаданных
    """
    def __init__(self,
                 p_type: str,
                 p_attrs: dict =None,
                 p_uuid: str =None
    ):
        """
        Конструктор

        :param p_type: тип объекта метаданных
        :param p_uuid: uuid объекта метаданных
        :param p_attrs: атрибуты объекта метаданных
        """
        self._type=p_type
        self._uuid=p_uuid
        self._attrs=p_attrs

        self.l_uuid_copy=copy.copy(uuid.uuid4()) # для того, чтобы uuid не изменялся каждый раз при вызове

    @property
    def type(self) -> str:
        """
        Тип объекта метаданных
        """
        l_meta_object=self._type.lower()
        if l_meta_object not in C_META_TABLES:
            AbaseError(p_error_text="There's no object " + l_meta_object + " in metadata", p_module="Metadata",
                  p_class="MetaObject", p_def="type").raise_error()
        return l_meta_object

    @property
    def uuid(self) -> uuid:
        """
        Id объекта метаданных
        """
        if self._uuid is None:
            return self.l_uuid_copy
        return self._uuid

    @property
    def attrs(self):
        """
        Атрибуты объекта метаданных
        """
        # проверяем на корректность
        self.attrs_checker()
        return self._attrs


    def attrs_checker(self):
        """
        Проверка атрибутов объекта метаданных
        """
        l_all_attrs_dict=C_META_ATTRIBUTES.get(self.type,None) # достаем атрибуты объекта и их свойства
        l_all_attrs_list=list(l_all_attrs_dict.keys()) # список всех атрибутов объекта
        l_attr_list = list(self._attrs.keys()) # список указанных атрибутов объекта
        l_necessary_attrs_list=[]# необходимые атрибуты объекта
        # проверка, что все необходимые атрибуты указаны
        for i_attr in l_all_attrs_list: # собираем список необходимых атрибутов объекта
            if l_all_attrs_dict.get(i_attr,None).get(C_NOT_NULL,None)==1:
                l_necessary_attrs_list.append(i_attr)
        for i_attr in l_necessary_attrs_list:
            if i_attr not in l_attr_list or self._attrs.get(i_attr,None) is None:
                AbaseError(p_error_text="Attribute "+i_attr+" is missed", p_module="Metadata", p_class="MetaObject",
                      p_def="attrs_checker").raise_error()
        for i_attr in l_attr_list:
            # проверка, что атрибут указан верно
            if i_attr not in l_all_attrs_list:
                AbaseError(p_error_text="Attribute " + str(i_attr) + " is incorrect", p_module="Metadata", p_class="MetaObject",
                      p_def="attrs_checker").raise_error()
            # проверка, что тип данных у атрибутов указан верно
            if type(self._attrs.get(i_attr,None)).__name__!=l_all_attrs_dict.get(i_attr,None).get(C_TYPE_VALUE) and self._attrs.get(i_attr,None) is not None:
                AbaseError(p_error_text="Attribute value" + str(i_attr) + " " + str(self.uuid) + " is incorrect", p_module="Metadata",
                      p_class="MetaObject", p_def="attrs_checker").raise_error()
            # проверка на дубль значения атрибута (только у ключевых атрибутов метаданных)
            if l_all_attrs_dict.get(i_attr, None).get(C_PK)==1: # если атрибут ключевой
                # достаем в объекты метаданных с таким же значением атрибута
                l_meta_attr_objs=search_object(
                    p_type=self.type,
                    p_attrs={
                        i_attr:self._attrs.get(i_attr,None)
                    }
                )
                l_meta_attr_objs_uuid_list=[] # добавляем все uuid полученных объектов в список
                for i_meta_attr_obj in l_meta_attr_objs:
                    l_meta_attr_objs_uuid_list.append(i_meta_attr_obj.uuid)
                # если уже есть объекты метаданных с таким значением ключевого атрибута и их uuid отличаются от self.uuid (объект не он сам)
                if l_meta_attr_objs.__len__()>0 and self.uuid not in l_meta_attr_objs_uuid_list:
                    AbaseError(p_error_text="Object " + self.type + " with "+i_attr+"="+str(self._attrs.get(i_attr, None)) + " already exists",
                          p_module="Metadata", p_class="MetaObject", p_def="attrs_checker").raise_error()