"""
Работа с ХД
"""
import sys
import dwh_config
from SystemObjects import Constant as const
import Driver
import Postgresql as pgsql
import FileWorker as fw
import copy
import uuid
import Metadata as meta
from Source import Source as Source

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
        if l_dbms_type not in const('C_AVAILABLE_DWH_LIST').constant_value:
            sys.exit("AnchorBase не умеет работать с СУБД "+l_dbms_type) #TODO: реализовать вывод ошибок, как сделал Рустем
        return l_dbms_type

    @property
    def dbms(self):
        """
        Модуль СУБД
        """
        if self.dbms_type==const('C_POSTGRESQL').constant_value:
            return pgsql


    def sql_exec(self, p_sql: str, p_result: int =1):
        """
        Выполнение запроса на стороне ХД

        :param p_sql: SQL-выражение
        :param p_result: Признак наличия результата запроса (по умолчанию 1)
        """
        l_sql_result=self.dbms.sql_exec(
            p_server=self.server,
            p_database=self.database,
            p_user=self.user,
            p_password=self.password,
            p_port=self.port,
            p_sql=p_sql,
            p_result=p_result
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
            p_file_path=const('C_CONFIG_FILE_PATH').constant_value,
            p_file_body=l_config
        ).write_file()



def _obj_meta_attrs(p_id: str, p_type: str):
    """
    Находит атрибуты существующих в метаданных объектов

    :param p_id: Id объекта метаданных
    """
    l_attr_dict={} # словарь для атрибутов источника

    l_meta_objs=meta.search_object(
        p_type=const(p_type).constant_value,
        p_uuid=[p_id]
    ) # достаем метаданные источника
    # проверяет на наличие источника в метаданных
    if l_meta_objs.__len__()==0:
        sys.exit("Нет "+p_type+" с указанным id "+p_id)
    else:
        l_attr_dict=l_meta_objs[0].attrs
    return l_attr_dict

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

def _get_object(p_id, p_class_name: str, p_type: str=None):
    """
    Достает сущность/сущности из метаданных

    :param p_id: id/список id сущности
    """
    if not p_id: # возвращаем пусто, если ничего не пришло
        return None
    if type(p_id).__name__.lower()=="uuid": # если пришле один id
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


def get_values_sql(p_data_frame: list, p_cast_list: dict):
    """
    Возвращает преобразованный data frame в конструкцию values (...,...),

    :param p_data_frame: data frame
    :param p_cast_list: словарь с порядковыми номерами атрибутов и конструкцией cast для каждого атрибута
    """
    l_sql=""
    for i_row in p_data_frame:
        i=0
        l_sql=l_sql+"("
        for i_column in i_row:
            if p_cast_list.get(i,None) is None:
                sys.exit("Нет указания cast для атрибута "+str(i)) #TODO: реализовать вывод ошибок, как сделал Рустем
            i_column="'"+str(i_column).replace("'","''")+"'" # экранируем одинарную кавычку ' в строке
            l_sql=l_sql+p_cast_list.get(i,None).replace(str(i),i_column,1)+","
            i=i+1
        l_sql=l_sql[:-1]+"),"
    return l_sql[:-1]

def _object_class_checker(p_object: object, p_class_name: str):
    """
    Проверяет наименование класса объекта

    :param p_object: объект класса
    """
    if type(p_object).__name__.lower()!=p_class_name.lower():
        sys.exit("Объект не является объектом класса "+p_class_name) # TODO переделать


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
                sys.exit("Объект не является объектом класса "+p_class_name)
            _object_class_checker(p_object=i_object, p_class_name=p_class_name)
    else:
        _object_class_checker(p_object=p_object, p_class_name=p_class_name) # в остальных случаях проверяем объект


def _get_attribute_type(p_attribute: object):
    """
    Получает
    :param p_attribute:
    """
    if not p_attribute:
        sys.exit("Передан пустой атрибут")
    if p_attribute.type==const('C_ENTITY_COLUMN').constant_value:
        return "entity_attribute"
    if p_attribute.type==const('C_QUEUE_COLUMN').constant_value:
        return "source_attribute"
    if p_attribute.type==const('C_IDMAP_COLUMN').constant_value:
        return "idmap_attribute"
    if p_attribute.type==const('C_ANCHOR_COLUMN').constant_value:
        return "anchor_attribute"
    if p_attribute.type==const('C_ATTRIBUTE_COLUMN').constant_value:
        return "attribute_table_attribute"
    if p_attribute.type==const('C_TIE_COLUMN').constant_value:
        return "tie_attribute"

def _get_table_attribute_property(p_table: object):
    """
    Получает атрибуты таблицы в зависимости от ее типа

    :param p_table: таблица
    """
    if p_table.type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
        return getattr(p_table, "source_attribute")
    if p_table.type==const('C_IDMAP_TABLE_TYPE_NAME').constant_value:
        return getattr(p_table, "idmap_attribute")
    if p_table.type==const('C_ANCHOR_TABLE_TYPE_NAME').constant_value:
        return getattr(p_table, "anchor_attribute")
    if p_table.type==const('C_ATTRIBUTE_TABLE_TYPE_NAME').constant_value:
        return getattr(p_table, "attribute_table_attribute")
    if p_table.type==const('C_TIE_TABLE_TYPE_NAME').constant_value:
        return getattr(p_table, "tie_attribute")
    if p_table.type==const('C_ENTITY').constant_value:
        return getattr(p_table, "entity_attribute")

def _get_table_property(p_table: object):
    """
    Получает наименование свойства таблицы в зависимости от ее типа

    :param p_table: таблица
    """
    if p_table.type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
        return "source_table"
    if p_table.type==const('C_IDMAP_TABLE_TYPE_NAME').constant_value:
        return "idmap"
    if p_table.type==const('C_ANCHOR_TABLE_TYPE_NAME').constant_value:
        return "anchor"
    if p_table.type==const('C_ATTRIBUTE_TABLE_TYPE_NAME').constant_value:
        return "attribute_table"
    if p_table.type==const('C_TIE_TABLE_TYPE_NAME').constant_value:
        return "tie_table"
    if p_table.type==const('C_ENTITY').constant_value:
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
        sys.exit("Похоже нельзя добавить больше одного атрибута")
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



def add_attribute(p_table: object, p_attribute: object):
    """
    Добавляет дочерний объект в родительский объект

    :param p_table: дочерний объект
    :param p_attribute_list: лист из дочерних объектов уже добавленных в родительский объект
    """
    # проверка
    _object_class_checker(p_class_name="Attribute", p_object=p_attribute)
    # добавляем атрибут в таблицу
    _add_object(
        p_parent_object=p_table,
        p_object=p_attribute,
        p_object_type=_get_attribute_type(p_attribute)
    )
    # добавляем таблицу в атрибут
    setattr(p_attribute, _get_table_property(p_table=p_table), p_table)


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
        sys.exit("Удалены все атрибуты") #TODO: переделать
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
    return const('C_TABLE_NAME_POSTFIX').constant_value.get(p_table_type,None)

def create_table_ddl(p_table: object):
    """
    Генерирует DDL таблицы
    :param p_table: таблица
    """
    l_schema=const('C_SCHEMA_TABLE_TYPE').constant_value.get(p_table.type,None) # схема таблицы
    p_attribute_sql="" # перечисление атрибутов
    p_attribute_partition_sql="" # указание атрибута для партиции
    for i_attribute in _get_table_attribute_property(p_table=p_table): # проходимся по всем атрибутам и формируем ddl
        p_attribute_sql=p_attribute_sql+"\n"+'"'+str(i_attribute.id)+'"'+" AS "+i_attribute.datatype.data_type_sql+","
        if i_attribute.attribute_type==const('C_FROM_TYPE_NAME').constant_value:
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
    l_schema=const('C_SCHEMA_TABLE_TYPE').constant_value.get(p_table.type,None) # схема таблицы
    p_attribute_sql="" # перечисление атрибутов
    for i_attribute in _get_table_attribute_property(p_table=p_table):
        p_attribute_sql=p_attribute_sql+"\n"+'"'+str(i_attribute.id)+'"'+" AS "+'"'+i_attribute.name+'"'+","
    p_attribute_sql=p_attribute_sql[:-1] # убираем последнюю запятую
    p_attribute_sql=p_attribute_sql[1:] # убираем первый перенос строк
    l_sql="CREATE OR REPLACE VIEW "+l_schema+"."+'"'+p_table.name+'"'+" AS \n"+p_attribute_sql\
          +"\nFROM "+l_schema+"."+'"'+str(p_table.id)+'";'
    return l_sql

def get_source_table_etl(p_source_table: object, p_attribute_value: list, p_etl_id: int):
    """
    ETL таблицы источника

    :param p_source_table: объект класса SourceTable
    :param p_etl: id etl процесса
    :param p_attribute_value: значения атрибутов, полученные с источника
    """
    l_attribute_name_list=[]
    l_update_attribute_sql=""
    l_etl_update_sql=""
    for i_attribute in _get_table_attribute_property(p_table=p_source_table):
        if i_attribute.attribute_type not in ( # атрибуты etl и update добавляются в конец
                [
                    const('C_ETL_TYPE_NAME').constant_value,
                    const('C_UPDATE_TYPE_NAME').constant_value
                ]
        ):
            l_attribute_name_list.append(i_attribute.name)
        elif i_attribute.attribute_type==const('C_UPDATE_TYPE_NAME').constant_value:
            l_update_attribute_sql='"'+str(i_attribute.id)+'"'
        elif i_attribute.attribute_type==const('C_ETL_TYPE_NAME').constant_value:
            l_etl_update_sql='"'+str(i_attribute.id)+'"'
    l_attribute_name_list.sort() # сортируем по наименованию
    # сортируем id в соответствии с наименованием атрибутов
    l_attribute_sql="\n\t\t"
    for i_attribute_name in l_attribute_name_list:
        for i_attribute in _get_table_attribute_property(p_table=p_source_table):
            if i_attribute.name==i_attribute_name:
                l_attribute_sql=l_attribute_sql+'"'+str(i_attribute.id)+'"'+",\n\t\t"
    l_attribute_sql=l_attribute_sql+l_update_attribute_sql+",\n\t\t"+l_etl_update_sql+"\n\t"

    l_attribute_value_sql=""
    for i_row in p_attribute_value: # проходимся по строкам
        l_attribute_value_sql=l_attribute_value_sql+"(\n\t\t"
        for i_column in i_row: # проходимся по столбцам
            # экранируем одинарные кавычки
            l_attribute_value_sql=l_attribute_value_sql+"CAST('"+str(i_column).replace("'","''")+"' AS "\
                                  +const('C_VARCHAR').constant_value+"(10000)),\n\t\t"
        l_attribute_value_sql=l_attribute_value_sql+"CAST("+str(p_etl_id)+" AS BIGINT)\n\t"
        l_attribute_value_sql=l_attribute_value_sql+"),\n\t"
    l_attribute_value_sql=l_attribute_value_sql[:-3] # убираем последнюю запятую, перенос строки и таб
    l_etl=Connection().dbms.get_source_table_etl(
        p_source_table_id=p_source_table.id,
        p_source_attribute=l_attribute_sql,
        p_source_attribute_value=l_attribute_value_sql
    )
    return l_etl

def get_idmap_etl(
        p_idmap: object,
        p_etl_id: str
):
    """
    Генерирует скрипт ETL для таблицы Idmap

    :param p_idmap: объект класса Idmap
    :param p_etl_id: id etl процесса
    """
    l_etl=[]
    l_idmap_nk_column=None
    l_idmap_rk_column=None
    l_etl_column=None
    for i_attribute in _get_table_attribute_property(p_table=p_idmap):
        if i_attribute.attribute_type==const('C_RK_TYPE_NAME').constant_value:
            l_idmap_rk_column=i_attribute.id
        if i_attribute.attribute_type==const('C_NK_TYPE_NAME').constant_value:
            l_idmap_nk_column=i_attribute.id
        if i_attribute.attribute_type==const('C_ETL_TYPE_NAME').constant_value:
            l_etl_column=i_attribute.id
    for i_source_table in p_idmap.entity.source_table:
        l_column_nk_sql=""
        for i_column_nk in p_idmap.source_attribute_nk:
            if i_source_table.id==i_column_nk.id:
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
        if i_attribute.attribute_type==const('C_RK_TYPE_NAME').constant_value:
            l_anchor_rk_id=i_attribute.id
        if i_attribute.attribute_type==const('C_SOURCE_TYPE_NAME').constant_value:
            l_anchor_source_id=i_attribute.id
        if i_attribute.attribute_type==const('C_ETL_TYPE_NAME').constant_value:
            l_anchor_etl_id=i_attribute.id
    for i_attribute in p_anchor.entity.idmap.idmap_attribute:
        if i_attribute.attribute_type==const('C_RK_TYPE_NAME').constant_value:
            l_idmap_rk_id=i_attribute.id
        if i_attribute.attribute_type==const('C_NK_TYPE_NAME').constant_value:
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

def get_attribute_etl(p_attribute_table: object, p_etl_id: str):
    """
    Генерирует etl скрипт для таблицы attribute

    :param p_attribute_table: объект класса  AttributeTable
    :param p_etl_id: id etl процесса
    """
    l_etl=[]
    l_attribute_rk_id=None
    l_attribute_column_id=None
    l_from_dttm_id=None
    l_to_dttm_id=None
    l_etl_id=None
    l_idmap_rk_id=None
    l_idmap_nk_id=None
    l_column_value_datatype=None
    for i_attribute in _get_table_attribute_property(p_table=p_attribute_table):
        if i_attribute.attribute_type==const('C_RK_TYPE_NAME').constant_value:
            l_attribute_rk_id=i_attribute.id
        if i_attribute.attribute_type==const('C_VALUE_TYPE_NAME').constant_value:
            l_attribute_column_id=i_attribute.id
            l_column_value_datatype=i_attribute.datatype.data_type_sql
        if i_attribute.attribute_type==const('C_FROM_TYPE_NAME').constant_value:
            l_from_dttm_id=i_attribute.id
        if i_attribute.attribute_type==const('C_TO_TYPE_NAME').constant_value:
            l_to_dttm_id=i_attribute.id
        if i_attribute.attribute_type==const('C_ETL_TYPE_NAME').constant_value:
            l_etl_id=i_attribute.id
    for i_attribute in p_attribute_table.entity.idmap.idmap_attribute:
        if i_attribute.attribute_type==const('C_RK_TYPE_NAME').constant_value:
            l_idmap_rk_id=i_attribute.id
        if i_attribute.attribute_type==const('C_NK_TYPE_NAME').constant_value:
            l_idmap_nk_id=i_attribute.id
    for i_source_attribute in p_attribute_table.entity_attribute.source_attribute:
        l_column_nk_sql=""
        for i_column_nk in p_attribute_table.entity.idmap.source_attribute_nk:
            if i_source_attribute.source_table.id==i_column_nk.source_table.id:
                l_column_nk_sql=l_column_nk_sql+"CAST("+'"'+str(i_column_nk.id)+'"'+" AS VARCHAR(4000))\n\t\t||'@@'||\n\t\t"
        l_column_value_id=i_source_attribute.id
        l_update_timestamp_id=None
        for i_source_attribute_all in i_source_attribute.source_table.source_attribute:
            if i_source_attribute_all.attribute_type==const('C_UPDATE_TYPE_NAME').constant_value:
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
        p_etl_id: str
):
    """
    Генерирует etl для таблицы Tie

    :param p_tie: объект класса Tie
    :param p_etl: id etl процесса
    """
    l_etl=[]
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
        if i_attribute.attribute_type==const('C_RK_TYPE_NAME').constant_value:
            l_anchor_rk_id=i_attribute.id
        if i_attribute.attribute_type==const('C_LINK_RK_TYPE_NAME').constant_value:
            l_link_anchor_rk_id=i_attribute.id
        if i_attribute.attribute_type==const('C_FROM_TYPE_NAME').constant_value:
            l_from_dttm_id=i_attribute.id
        if i_attribute.attribute_type==const('C_TO_TYPE_NAME').constant_value:
            l_to_dttm_id=i_attribute.id
        if i_attribute.attribute_type==const('C_ETL_TYPE_NAME').constant_value:
            l_etl_id=i_attribute.id
    for i_attribute in p_tie.entity.idmap.idmap_attribute:
        if i_attribute.attribute_type==const('C_RK_TYPE_NAME').constant_value:
            l_idmap_rk_id=i_attribute.id
        if i_attribute.attribute_type==const('C_NK_TYPE_NAME').constant_value:
            l_idmap_nk_id=i_attribute.id
    for i_attribute in p_tie.link_entity.idmap.idmap_attribute:
        if i_attribute.attribute_type==const('C_RK_TYPE_NAME').constant_value:
            l_link_idmap_rk_id=i_attribute.id
        if i_attribute.attribute_type==const('C_NK_TYPE_NAME').constant_value:
            l_link_idmap_nk_id=i_attribute.id
    for i_source_table in p_tie.source_table:
        l_column_nk_sql=""
        for i_column_nk in p_tie.entity.idmap.source_attribute_nk:
            if i_source_table.id==i_column_nk.source_table.id:
                l_column_nk_sql=l_column_nk_sql+"CAST("+'"'+str(i_column_nk.id)+'"'+" AS VARCHAR(4000))\n\t\t||'@@'||\n\t\t"
        l_link_column_nk_sql=""
        for i_entity_link_attribute in p_tie.entity_attribute:
            for i_column_nk in i_entity_link_attribute.source_attribute:
                if i_source_table.id==i_column_nk.source_table.id:
                    l_link_column_nk_sql=l_link_column_nk_sql+"CAST("+'"'+str(i_column_nk.id)+'"'+" AS VARCHAR(4000))\n\t\t||'@@'||\n\t\t"
        l_update_timestamp_id=None
        for i_source_attribute in i_source_table.source_attribute:
            if i_source_attribute.attribute_type==const('C_UPDATE_TYPE_NAME').constant_value:
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

class _DWHObject:
    """
    Обхъект ХД
    """

    def __init__(self,
                 p_type: str,
                 p_id: str =None,
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
                sys.exit("Нет "+self._type+" с указанным id "+self._id)
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
        l_meta_obj=self.object_attrs_meta.get(const('C_SOURCE').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Source")

        return self._source or l_object

    @source.setter
    def source(self, p_new_source):

        self._source=p_new_source

    @property
    def source_table(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="SourceTable",p_object=self._source_table)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_QUEUE_TABLE_TYPE_NAME').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="SourceTable")

        return self._source_table or l_object

    @source_table.setter
    def source_table(self, p_new_source_table):

        self._source_table=p_new_source_table

    @property
    def source_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._source_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_QUEUE_COLUMN').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute", p_type=const('C_QUEUE_COLUMN').constant_value)

        return self._source_attribute or l_object

    @source_attribute.setter
    def source_attribute(self, p_new_source_attribute):

        self._source_attribute=p_new_source_attribute

    @property
    def entity(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Entity",p_object=self._entity)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_ENTITY').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Entity")

        return self._entity or l_object

    @entity.setter
    def entity(self, p_new_entity):

        self._entity=p_new_entity

    @property
    def link_entity(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Entity",p_object=self._link_entity)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_LINK_ENTITY').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Entity")

        return self._link_entity or l_object

    @link_entity.setter
    def link_entity(self, p_new_link_entity):

        self._link_entity=p_new_link_entity

    @property
    def entity_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._entity_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_ENTITY_COLUMN').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=const('C_ENTITY_COLUMN').constant_value)

        return self._entity_attribute or l_object

    @entity_attribute.setter
    def entity_attribute(self, p_new_entity_attribute):

        self._entity_attribute=p_new_entity_attribute

    @property
    def idmap(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Idmap",p_object=self._idmap)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_IDMAP_TABLE_TYPE_NAME').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Idmap")

        return self._idmap or l_object

    @idmap.setter
    def idmap(self, p_new_idmap):

        self._idmap=p_new_idmap

    @property
    def idmap_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._idmap_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_IDMAP_COLUMN').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=const('C_IDMAP_COLUMN').constant_value)

        return self._idmap_attribute or l_object

    @idmap_attribute.setter
    def idmap_attribute(self, p_new_idmap_attribute):

        self._idmap_attribute=p_new_idmap_attribute

    @property
    def anchor(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Anchor",p_object=self._anchor)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_ANCHOR_TABLE_TYPE_NAME').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Anchor")

        return self._anchor or l_object

    @anchor.setter
    def anchor(self, p_new_anchor):

        self._anchor=p_new_anchor

    @property
    def anchor_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._anchor_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_ANCHOR_COLUMN').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=const('C_ANCHOR_COLUMN').constant_value)

        return self._anchor_attribute or l_object

    @anchor_attribute.setter
    def anchor_attribute(self, p_new_anchor_attribute):

        self._anchor_attribute=p_new_anchor_attribute

    @property
    def attribute_table(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="AttributeTable",p_object=self._attribute_table)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_ATTRIBUTE_TABLE_TYPE_NAME').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="AttributeTable")

        return self._attribute_table or l_object

    @attribute_table.setter
    def attribute_table(self, p_new_attribute_table):

        self._attribute_table=p_new_attribute_table

    @property
    def attribute_table_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._attribute_table_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_ATTRIBUTE_COLUMN').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=const('C_ATTRIBUTE_COLUMN').constant_value)

        return self._attribute_table_attribute or l_object

    @attribute_table_attribute.setter
    def attribute_table_attribute(self, p_new_attribute_table_attribute):

        self._attribute_table_attribute=p_new_attribute_table_attribute

    @property
    def tie(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Tie",p_object=self._tie)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_TIE_TABLE_TYPE_NAME').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Tie")

        return self._tie or l_object

    @tie.setter
    def tie(self, p_new_tie):

        self._tie=p_new_tie

    @property
    def tie_attribute(self):
        """
        Таблицы источники
        """
        # проверка объекта
        _class_checker(p_class_name="Attribute",p_object=self._tie_attribute)
        # собираем метаданные, если указаны
        l_meta_obj=self.object_attrs_meta.get(const('C_TIE_COLUMN').constant_value,None)
        # выбираем из метаданных
        l_object=_get_object(p_id=l_meta_obj, p_class_name="Attribute",p_type=const('C_TIE_COLUMN').constant_value)

        return self._tie_attribute or l_object

    @tie_attribute.setter
    def tie_attribute(self, p_new_tie_attribute):

        self._tie_attribute=p_new_tie_attribute

    @property
    def desc(self):
        """
        Описание атрибута
        """
        return self._desc or self.object_attrs_meta.get(const('C_DESC').constant_value,None)

    @desc.setter
    def desc(self, p_new_desc):

        self._desc=p_new_desc

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
        l_all_attribute_list=dir(self) # лист с атрибутами и переменными объекта
        l_constant_name="C_"+str(self.type).upper()+"_META_ATTRIBUTES" # имя константы, содержащей атрибуты метаданных объекта
        l_metadata_attributes=list(const(l_constant_name).constant_value.keys())

        # основные атрибуты
        l_json.update(
            {
                const('C_NAME').constant_value:self.name,
            }
        )
        # атрибуты объекта Attribute
        if type(self).__name__=="Attribute":
            l_json.update(
                {
                    const('C_TYPE_VALUE').constant_value:self.attribute_type,
                    const('C_DATATYPE').constant_value:self.datatype.data_type_name,
                    const('C_LENGTH').constant_value:self.datatype.data_type_length,
                    const('C_SCALE').constant_value:self.datatype.data_type_scale
                }
            )
            if self.type==const('C_ENTITY_COLUMN').constant_value: # если атрибут сущности
                l_json.update(
                    {
                        const('C_PK').constant_value:self.pk,
                    }
                )
                l_json.pop(const('C_TYPE_VALUE').constant_value) # убираем атрибут type

        # атрибуты queue таблицы
        if self.type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
            l_json.update(
                {
                    const('C_SCHEMA').constant_value:self.schema,
                    const('C_SOURCE_NAME').constant_value:self.name,
                    const('C_NAME').constant_value:self.queue_name # переопределяем наименование
                }
            )
        # если указана связанная queue таблица
        if self.source_table:
            l_json.update(
                {
                    const('C_QUEUE_TABLE_TYPE_NAME').constant_value:self.__get_property_id(p_property=self.source_table)
                }
            )
        # если указан атрибут источника
        if self.source_attribute:
            l_json.update(
                {
                    const('C_QUEUE_COLUMN').constant_value:self.__get_property_id(p_property=self.source_attribute)
                }
            )
        # если указан источник
        if self.source:
            l_json.update(
                {
                    const('C_SOURCE').constant_value:self.__get_property_id(p_property=self.source)
                }
            )
        # если указана сущность
        if self.entity:
            l_json.update(
                {
                    const('C_ENTITY').constant_value:self.__get_property_id(p_property=self.entity)
                }
            )
        # если указаны натуральные ключи
        if self.type==const('C_IDMAP_TABLE_TYPE_NAME').constant_value:
            l_json.update(
                {
                    const('C_ATTRIBUTE_NK').constant_value:self.__get_property_id(p_property=self.source_attribute_nk)
                }
            )
        # если заполнены атрибуты idmap
        if self.idmap_attribute:
            l_json.update(
                {
                    const('C_IDMAP_COLUMN').constant_value:self.__get_property_id(p_property=self.idmap_attribute)
                }
            )

        if self.idmap:
            l_json.update(
                {
                    const('C_IDMAP_TABLE_TYPE_NAME').constant_value:self.__get_property_id(p_property=self.idmap)
                }
            )

        # если заполнены атрибуты anchor
        if self.anchor_attribute:
            l_json.update(
                {
                    const('C_ANCHOR_COLUMN').constant_value:self.__get_property_id(p_property=self.anchor_attribute)
                }
            )
        # если указан якорь
        if self.anchor:
            l_json.update(
                {
                    const('C_ANCHOR_TABLE_TYPE_NAME').constant_value:self.__get_property_id(p_property=self.anchor)
                }
            )
        # Если указана таблица атрибут
        if self.attribute_table:
            l_json.update(
                {
                    const('C_ATTRIBUTE_TABLE_TYPE_NAME').constant_value:self.__get_property_id(p_property=self.attribute_table)
                }
            )
        # Если указаны атрибуты таблицы атрибут
        if self.attribute_table_attribute:
            l_json.update(
                {
                    const('C_ATTRIBUTE_COLUMN').constant_value:self.__get_property_id(p_property=self.attribute_table_attribute)
                }
            )
        # Если указаны атрибуты сущности
        if self.entity_attribute:
            l_json.update(
                {
                    const('C_ENTITY_COLUMN').constant_value:self.__get_property_id(p_property=self.entity_attribute)
                }
            )
        # Если указан tie
        if self.tie:
            l_json.update(
                {
                    const('C_TIE_TABLE_TYPE_NAME').constant_value:self.__get_property_id(p_property=self.tie)
                }
            )
        # Если указаны атрибуты tie
        if self.tie_attribute:
            l_json.update(
                {
                    const('C_TIE_COLUMN').constant_value:self.__get_property_id(p_property=self.tie_attribute)
                }
            )
        # Если указана связанная сущность
        if self.link_entity:
            l_json.update(
                {
                    const('C_LINK_ENTITY').constant_value:self.__get_property_id(p_property=self.link_entity)
                }
            )

        return l_json

    def create_metadata(self):
        """
        Записывает метаданные объекта
        """
        meta.create_object(
            meta.MetaObject(
                p_type=self.type,
                p_uuid=str(self.id),
                p_attrs=self.metadata_json

            )
        )

    def update_metadata(self):
        """
        Обновляет метаданные объекта
        """
        meta.update_object(
            meta.MetaObject(
                p_type=self.type,
                p_uuid=str(self.id),
                p_attrs=self.metadata_json
            )
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


    @property
    def name(self):
        """
        Наименование атрибута
        """
        l_name=None
        if self._name is not None:
            l_name=self._name.lower()
        return l_name or self.object_attrs_meta.get(const('C_NAME').constant_value,None)

    @property
    def datatype(self):
        """
        Тип данных
        """
        l_datatype=self._datatype or self.object_attrs_meta.get(const('C_DATATYPE').constant_value,None)
        l_length=self._length or self.object_attrs_meta.get(const('C_LENGTH').constant_value,None)
        l_scale=self._scale or self.object_attrs_meta.get(const('C_SCALE').constant_value,None)
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
            if l_attribute_type not in const('C_ATTRIBUTE_TABLE_TYPE_LIST').constant_value:
                sys.exit("Некорректно задан тип атрибута "+str(l_attribute_type))
        return l_attribute_type or self.object_attrs_meta.get(const('C_TYPE_VALUE').constant_value,None)

    @property
    def pk(self):
        """
        Признак ключа
        """
        if self._pk is not None:
            return self._pk
        else:
            return self.object_attrs_meta.get(const('C_PK').constant_value,None)

    def __source_attribute_exist_checker(self):
        """
        Проверяет наличие атрибута в таблице источнике
        """
        if self.source_table.schema+"."+self.source_table.name+"."+self.name not in self.source_table.source.source_attributes \
                and self.attribute_type not in ([const('C_UPDATE_TYPE_NAME').constant_value,const('C_ETL_TYPE_NAME').constant_value]):
            sys.exit("На источнике на найден атрибут "+self.source_table.schema+"."+self.source_table.name+"."+self.name)


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
            p_type=const('C_ENTITY').constant_value,
            p_entity_attribute=p_entity_attribute,
            p_source=p_source,
            p_source_table=p_source_table,
            p_idmap=p_idmap,
            p_anchor=p_anchor,
            p_attribute_table=p_attribute_table,
            p_tie=p_tie,
            p_desc=p_desc
        )
        self._name=p_name


    @property
    def name(self):
        """
        Наименование сущности
        """
        l_name=None
        if self._name is not None:
            l_name=self._name.lower()
        return l_name or self.object_attrs_meta.get(const('C_NAME').constant_value,None)


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
            p_type=const('C_QUEUE_TABLE_TYPE_NAME').constant_value,
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
        return l_name or self.object_attrs_meta.get(const('C_SOURCE_NAME').constant_value,None)

    @property
    def schema(self):
        """
        Наименование схемы таблицы источника
        """
        l_schema=None
        if self._schema is not None:
            l_schema=self._schema.lower()
        return l_schema or self.object_attrs_meta.get(const('C_SCHEMA').constant_value,None)


    @property
    def queue_name(self):
        """
        Наименование таблицы queue
        """
        return self.source.name.replace(" ","_")+"_"+self.schema+"_"+self.name+"_"+const('C_QUEUE_TABLE_TYPE_NAME').constant_value

    @property
    def increment(self):
        """
        Инкремент
        """
        if self._increment:
            if type(self._increment).__name__!="Attribute":
                sys.exit("p_increment не является объектом класса Attribute") #TODO переделать
            # if self._increment.datatype.data_type_name!=const('C_TIMESTAMP_DBMS').constant_value.get(self.source.type):
            #     sys.exit("У инкремента некорректный тип данных") #TODO переделать
        l_increment_meta_obj=self.object_attrs_meta.get(const('C_INCREMENT').constant_value,None)
        l_increment=None
        if l_increment_meta_obj:
            l_increment=Attribute(
                p_id=l_increment_meta_obj,
                p_type="queue_attr"
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
            p_type=const('C_QUEUE_TABLE_TYPE_NAME').constant_value+"_"+const('C_INCREMENT').constant_value,
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
        l_attribute_sql=""
        l_attribute_name_list=[]
        for i_attribute in self.source_attribute:
            l_attribute_name_list.append(i_attribute.name)
        l_attribute_name_list.sort() # сортируем по наименоваию
        for i_attribute in l_attribute_name_list:
            l_attribute_sql=l_attribute_sql+"\n"+'"'+i_attribute+'"'+","
        l_attribute_sql=l_attribute_sql[1:] # убираем первый перенос строки
        l_attribute_sql=l_attribute_sql[:-1] # убираем последнюю запятую
        l_increment_sql=""
        l_timestamp_type=const('C_TIMESTAMP_DBMS').constant_value.get(self.source.type)
        if self.increment:
            l_increment_attr_sql='"'+self.increment.name+'"'+" AS update_timestamp"
        else:
            l_increment_attr_sql=self.source.current_timestamp_sql+" AS update_timestamp"
        if self.last_increment:
            l_increment_sql="\nWHERE "+'"'+self.increment.name+'"'+">CAST('"+self.last_increment+"' AS "+l_timestamp_type+")"
        l_sql="SELECT\n"+l_attribute_sql+"\n"+l_increment_attr_sql+"\nFROM "+'"'+self.schema+'"'+"."+'"'+self.name+'"'\
              +l_increment_sql+";"
        return l_sql

    def __create_source_attribute(self):
        """
        Создает технические атрибуты для source таблицы
        """
        update_column=Attribute(
            p_name=const('C_UPDATE_TIMESTAMP_NAME').constant_value,
            p_type=const('C_QUEUE_COLUMN').constant_value,
            p_datatype=const('C_TIMESTAMP_DBMS').constant_value.get(Connection().dbms_type),
            p_attribute_type=const('C_UPDATE_TYPE_NAME').constant_value,
            p_source_table=self
        )
        etl_column=Attribute(
            p_name=const('C_ETL_ATTRIBUTE_NAME').constant_value,
            p_type=const('C_QUEUE_COLUMN').constant_value,
            p_datatype=const('C_BIGINT').constant_value,
            p_attribute_type=const('C_ETL_TYPE_NAME').constant_value,
            p_source_table=self
        )
        if not self.source_attribute:
            add_attribute(p_table=self, p_attribute=update_column)
            add_attribute(p_table=self, p_attribute=etl_column)
        else:
            l_attribute_type_list=[]
            for i_attribute in self.source_attribute:
                l_attribute_type_list.append(i_attribute.attribute_type)
            if const('C_UPDATE_TYPE_NAME').constant_value not in l_attribute_type_list:
                add_attribute(p_table=self, p_attribute=update_column)
            if const('C_ETL_TYPE_NAME').constant_value not in l_attribute_type_list:
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
            p_type=const('C_IDMAP_TABLE_TYPE_NAME').constant_value,
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
        if self.entity.name:
            return str(self.entity.name).lower()+_get_table_postfix(p_table_type=const('C_IDMAP_TABLE_TYPE_NAME').constant_value)
        else:
            return self.object_attrs_meta.get(const('C_NAME').constant_value)

    def __create_idmap_attribute(self):
        """
        Создает фиксированные атрибуты для idmap, если их нет в метаданных
        """
        if not self.idmap_attribute:
            rk_column=Attribute(
                p_name=self.entity.name+"_"+const('C_RK_TYPE_NAME').constant_value,
                p_type=const('C_IDMAP_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_attribute_type=const('C_RK_TYPE_NAME').constant_value,
                p_idmap=self
            )
            nk_column=Attribute(
                p_name=self.entity.name+"_"+const('C_NK_TYPE_NAME').constant_value,
                p_type=const('C_IDMAP_COLUMN').constant_value,
                p_datatype=const('C_VARCHAR').constant_value,
                p_length=4000,
                p_attribute_type=const('C_NK_TYPE_NAME').constant_value,
                p_idmap=self
            )
            etl_column=Attribute(
                p_name=self.entity.name+"_"+const('C_ETL_TYPE_NAME').constant_value,
                p_type=const('C_IDMAP_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_length=4000,
                p_attribute_type=const('C_ETL_TYPE_NAME').constant_value,
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
        l_attribute_meta_obj=self.object_attrs_meta.get(const('C_ATTRIBUTE_NK').constant_value)
        if l_attribute_meta_obj:
            for i_attribute in l_attribute_meta_obj:
                l_attribute.append(
                    Attribute(
                        p_id=i_attribute,
                        p_type=const('C_QUEUE_TABLE_TYPE_NAME').constant_value
                    )
            )
        return self._source_attribute_nk or l_attribute

    @source_attribute_nk.setter
    def source_attribute_nk(self, p_new_source_attribute_nk):
        self._source_attribute_nk=p_new_source_attribute_nk






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
            p_type=const('C_ANCHOR_TABLE_TYPE_NAME').constant_value,
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
        if self.entity.name:
           return str(self.entity.name).lower()+_get_table_postfix(p_table_type=const('C_ANCHOR_TABLE_TYPE_NAME').constant_value)
        else:
           return self.object_attrs_meta.get(const('C_NAME').constant_value)

    def __create_anchor_attribute(self):
        """
        Генерирует атрибуты для anchor, если их нет в метаданных
        """
        if not self.anchor_attribute:
            rk_column=Attribute(
                p_name=self.entity.name+"_"+const('C_RK_TYPE_NAME').constant_value,
                p_type=const('C_ANCHOR_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_attribute_type=const('C_RK_TYPE_NAME').constant_value,
                p_anchor=self
            )
            source_system_id=Attribute(
                p_name=const('C_SOURCE_ATTRIBUTE_NAME').constant_value,
                p_type=const('C_ANCHOR_COLUMN').constant_value,
                p_datatype=const('C_INT').constant_value,
                p_attribute_type=const('C_SOURCE_TYPE_NAME').constant_value,
                p_anchor=self
            )
            etl_id=Attribute(
                p_name=const('C_ETL_ATTRIBUTE_NAME').constant_value,
                p_type=const('C_ANCHOR_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_attribute_type=const('C_ETL_TYPE_NAME').constant_value,
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
            p_type=const('C_ATTRIBUTE_TABLE_TYPE_NAME').constant_value,
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
        if self.entity_attribute.name:
            return self.entity.name+"_"+self.entity_attribute.name\
                   +_get_table_postfix(p_table_type=const('C_ATTRIBUTE_TABLE_TYPE_NAME').constant_value)
        else:
            return self.object_attrs_meta.get(const('C_NAME').constant_value)

    def __create_attributetable_attribute(self):
        """
         Генерирует атрибуты для attribute, если их нет в метаданных
        """
        if not self.attribute_table_attribute:
            rk_column=Attribute(
                p_name=self.entity.name+"_"+const('C_RK_TYPE_NAME').constant_value,
                p_type=const('C_ATTRIBUTE_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_attribute_type=const('C_RK_TYPE_NAME').constant_value,
                p_attribute_table=self
            )
            value_column=Attribute(
                p_name=self.entity_attribute.name,
                p_type=const('C_ATTRIBUTE_COLUMN').constant_value,
                p_datatype=self.entity_attribute.datatype.data_type_name,
                p_length=self.entity_attribute.datatype.data_type_length,
                p_scale=self.entity_attribute.datatype.data_type_scale,
                p_attribute_type=const('C_VALUE_TYPE_NAME').constant_value,
                p_attribute_table=self
            )
            from_dttm=Attribute(
                p_name=const('C_FROM_ATTRIBUTE_NAME').constant_value,
                p_type=const('C_ATTRIBUTE_COLUMN').constant_value,
                p_datatype=const('C_TIMESTAMP').constant_value,
                p_attribute_type=const('C_FROM_TYPE_NAME').constant_value,
                p_attribute_table=self
            )
            to_dttm=Attribute(
                p_name=const('C_TO_ATTRIBUTE_NAME').constant_value,
                p_type=const('C_ATTRIBUTE_COLUMN').constant_value,
                p_datatype=const('C_TIMESTAMP').constant_value,
                p_attribute_type=const('C_TO_TYPE_NAME').constant_value,
                p_attribute_table=self
            )
            etl_id=Attribute(
                p_name=const('C_ETL_ATTRIBUTE_NAME').constant_value,
                p_type=const('C_ATTRIBUTE_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_attribute_type=const('C_ETL_TYPE_NAME').constant_value,
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
            p_type=const('C_TIE_TABLE_TYPE_NAME').constant_value,
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
               _get_table_postfix(p_table_type=const('C_TIE_TABLE_TYPE_NAME').constant_value)
        return l_name

    def __create_tie_attribute(self):
        """
        Генерирует атрибуты для tie, если их нет в метаданных
        """
        if not self.tie_attribute:
            rk_column=Attribute( # атрибутом типом rk становится первый по порядку
                p_name=self.entity.name+"_"+const('C_RK_TYPE_NAME').constant_value,
                p_type=const('C_TIE_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_attribute_type=const('C_RK_TYPE_NAME').constant_value,
                p_tie=self
            )
            link_rk_column=Attribute( # атрибутом типом link_rk становится второй по порядку
                p_name=self.link_entity.name+"_"+const('C_RK_TYPE_NAME').constant_value,
                p_type=const('C_TIE_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_attribute_type=const('C_LINK_RK_TYPE_NAME').constant_value,
                p_tie=self
            )
            from_dttm=Attribute(
                p_name=const('C_FROM_ATTRIBUTE_NAME').constant_value,
                p_type=const('C_TIE_COLUMN').constant_value,
                p_datatype=const('C_TIMESTAMP').constant_value,
                p_attribute_type=const('C_FROM_TYPE_NAME').constant_value,
                p_tie=self
            )
            to_dttm=Attribute(
                p_name=const('C_TO_ATTRIBUTE_NAME').constant_value,
                p_type=const('C_TIE_COLUMN').constant_value,
                p_datatype=const('C_TIMESTAMP').constant_value,
                p_attribute_type=const('C_TO_TYPE_NAME').constant_value,
                p_tie=self
            )
            etl_id=Attribute(
                p_name=const('C_ETL_ATTRIBUTE_NAME').constant_value,
                p_type=const('C_TIE_COLUMN').constant_value,
                p_datatype=const('C_BIGINT').constant_value,
                p_attribute_type=const('C_ETL_TYPE_NAME').constant_value,
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
        l_entity_tie=self.entity.tie
        l_link_entity_tie=self.link_entity.tie
        if l_entity_tie:
            l_entity_tie.append(self)
        else:
            l_entity_tie=[self]
        if l_link_entity_tie:
            l_entity_tie.append(self)
        else:
            l_link_entity_tie=[self]

        self.entity.tie=l_entity_tie
        self.link_entity.tie=l_link_entity_tie


class Job:
    """
    Загружает данные
    """
    def __init__(self, p_entity_id: list =None):
        """
        Конструктор
        :param p_entity_id: id сущности, таблицы которой требуется обновить
        """
        self._entity_id=p_entity_id

        self.l_job_id=copy.copy(uuid.uuid4()) # для того, чтобы uuid не изменялся каждый раз при вызове

    @property
    def id(self):
        """
        Id job-а
        """
        return self.l_job_id

    def get_table_uuid(self, p_table_type: str):
        """
        Получает все uuid таблиц определенного типа

        :param p_table_type: тип таблицы
        """
        # проверка заданного типа таблицы
        if p_table_type not in const('C_TABLE_TYPE_LIST').constant_value:
            sys.exit("Некорректный тип таблицы "+p_table_type)
        # выбираем uuid из метаданных
        l_table_uuid_list=meta.search_object(
            p_type=p_table_type,
            p_attrs={
                const('C_DELETED_META_ATTR').constant_value:0 # выбираем только неудаленные объекты
            }
        )
        return l_table_uuid_list


    def __get_table_etl(self):
        """
        Получает скрипт ETL загрузки данных таблицы
        """
        pass



