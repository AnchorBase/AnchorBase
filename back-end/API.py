"""
Взаимодействие с приложением
"""

from DWH import *
from Source import *
from Model import *
from Metadata import *
from Constants import *
import json


def get_source(p_source_name: str =None, p_source_id: str =None):
    """
    Выдает источники и их параметры подключения

    :param p_source_name: наименование источника (если требуется конкретный источник)
    :param p_source_id: id источника (если требуется конкретный источник)
    """
    l_source=[] # лист источников
    if p_source_id: # если указан id
        l_source.append(Source(p_id=p_source_id))
    elif p_source_name:
        l_source_meta=search_object(p_type=C_SOURCE, p_attrs={C_NAME:p_source_name})
        if l_source_meta.__len__()>0:
            l_source.append(Source(p_id=l_source_meta[0].uuid))
    else:  # если ничего не указано, выдает все существующие источники
        l_source_meta=search_object(p_type=C_SOURCE)
        if l_source_meta.__len__()>0:
            for i_source_meta in l_source_meta:
                l_source.append(Source(p_id=i_source_meta.uuid))
    l_json_object=[] # лист с объектами в формате json
    for i_source in l_source:
        l_attribute_dict={
            C_TYPE_VALUE:i_source.type,
            C_ID:str(i_source.id),
            C_NAME:i_source.name,
            C_DESC:i_source.description,
            C_SERVER:i_source.server,
            C_DATABASE:i_source.database,
            C_PORT:i_source.port,
            C_USER:i_source.user,
            C_PASSWORD:i_source.password,
            C_SOURCE_ID:i_source.source_id
        }
        l_json_object.append(
            _JsonObject(p_id=i_source.id, p_type=C_SOURCE, p_attribute=l_attribute_dict)
        )
    l_error=None
    if l_json_object.__len__()==0:
        l_json_object=None
        l_error="Ничего не найдено"
    return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body

def add_source(p_type: str, p_name: str, p_server: str, p_database: str, p_user: str, p_password: str, p_port: int, p_desc: str =None):
    """
    Добавляет новый источник

    :param p_type: тип источника
    :param p_name: наименование источника
    :param p_server: сервер
    :param p_database: база данных
    :param p_user: логин
    :param p_password: пароль
    :param p_port: порт
    :param p_desc: описание источника
    """
    l_source=Source(
        p_type=p_type,
        p_name=p_name,
        p_server=p_server,
        p_database=p_database,
        p_user=p_user,
        p_password=p_password,
        p_port=int(p_port),
        p_desc=p_desc
    )
    l_source.create_source()
    l_attribute_dict={
        C_TYPE_VALUE:l_source.type,
        C_ID:str(l_source.id),
        C_NAME:l_source.name,
        C_DESC:l_source.description,
        C_SERVER:l_source.server,
        C_DATABASE:l_source.database,
        C_PORT:l_source.port,
        C_USER:l_source.user,
        C_PASSWORD:l_source.password,
        C_SOURCE_ID:l_source.source_id
    }
    l_json_object=[_JsonObject(p_id=str(l_source.id), p_type=C_SOURCE, p_attribute=l_attribute_dict)]
    return _JsonOutput(p_json_object=l_json_object, p_message="Источник успешно добавлен").body

def update_source(
        p_id: str,
        p_type: str =None,
        p_name: str =None,
        p_server: str =None,
        p_database: str =None,
        p_user: str =None,
        p_password: str =None,
        p_port: int =None,
        p_desc: str =None
):
    """
    Изменяет параметры источника

    :param p_name: наименование источника
    :param p_type: тип источника
    :param p_server: сервер
    :param p_database: база данных
    :param p_user: логин
    :param p_password: пароль
    :param p_port: порт
    :param p_desc: описание источника
    """
    l_source=Source(
        p_id=p_id,
        p_type=p_type,
        p_name=p_name,
        p_server=p_server,
        p_database=p_database,
        p_user=p_user,
        p_password=p_password,
        p_port=p_port,
        p_desc=p_desc
    )
    l_source.update_source()
    return _JsonOutput(p_json_object=None, p_message="Источник успешно обновлен").body

def get_source_type():
    """
    Выдает все типы источников, с которым может работать AnchorBase
    """
    l_available_source_type=C_AVAILABLE_SOURCE_LIST
    l_source_type_str="" # все типы источников в строку
    for i_source_type in l_available_source_type:
        l_source_type_str+=i_source_type+","
    l_source_type_str=l_source_type_str[:-1] # убираем последнюю запятую
    return _JsonOutput(p_json_object=None, p_message="AnchorBase умеет работать со следующими типами источников: "+l_source_type_str).body


def get_entity(p_name: str =None, p_id: str =None):
    """
    Выдает информацию о созданной сущности

    :param p_name: наименование сущности
    """
    l_error=None
    l_json_object=None
    l_id=None
    if p_id:
        l_id=[str(p_id)]
    l_attr=None
    if p_name:
        l_attr={C_NAME:p_name}
    l_entity_meta=search_object(p_type=C_ENTITY, p_uuid=l_id, p_attrs=l_attr)
    if l_entity_meta.__len__()==0:
        l_error="Ничего не найдено"
    else:
        l_json_object=[]
        for i_entity in l_entity_meta:
            l_entity=Entity(p_id=str(i_entity.uuid))
            # формируем строку из источников
            l_entity_dict={
                C_ID:l_entity.id,
                C_NAME:l_entity.name,
                C_DESC:l_entity.desc
            }
            l_json_object.append(
                _JsonObject(p_type=C_ENTITY,p_id=str(l_entity.id), p_attribute=l_entity_dict)
            )
    return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body

def get_entity_attr(p_id: str =None, p_name: str =None, p_entity: str =None):
    """
    Выдает информацию об атрибутах сущностей

    :param p_id: id атрибута
    :param p_name: наименование атрибута
    :param p_entity: наименование сущности
    """
    l_error=None
    l_json_object=None
    l_id=None
    if p_id:
        l_id=[str(p_id)]
    l_attr=None
    if p_name or p_entity:
        l_attr={}
        if p_name:
            l_attr={C_NAME:p_name}
        if p_entity:
            # вытаскиваем id сущности
            l_entity_meta=search_object(p_type=C_ENTITY,p_attrs={C_NAME:p_entity})
            if l_entity_meta:
                l_entity=Entity(p_id=str(l_entity_meta[0].uuid))
                l_attr.update({C_ENTITY:l_entity.id})
            else:
                l_attr.update({C_ENTITY:C_DUMMY_UUID}) # если указано неправильное имя сущности - ничего не найдется
    l_ent_attr_meta=search_object(p_type=C_ENTITY_COLUMN, p_uuid=l_id, p_attrs=l_attr)
    if l_ent_attr_meta.__len__()==0:
        l_error="Ничего не найдено"
    else:
        l_json_object=[]
        for i_ent_attr in l_ent_attr_meta:
            l_ent_attr=Attribute(
                p_type=C_ENTITY_COLUMN,
                p_id=str(i_ent_attr.uuid)
            )
            l_ent_attr_dict={
                C_ID:l_ent_attr.id,
                C_NAME:l_ent_attr.name,
                C_DESC:l_ent_attr.desc,
                C_DATATYPE:l_ent_attr.datatype.data_type_sql,
                C_PK:l_ent_attr.pk,
                C_ENTITY:l_ent_attr.entity.name
            }
            l_json_object.append(
                _JsonObject(p_type=C_ENTITY_COLUMN,p_id=str(l_ent_attr.id), p_attribute=l_ent_attr_dict)
            )
    return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body

def get_attr_source(p_id: str =None, p_name: str =None, p_entity: str =None, p_source_id: str =None):
    """
    Получение информации по источникам атрибутов сущностей

    :param p_id: id атрибута
    :param p_name: наименование атрибута
    :param p_entity: наименование сущности
    :param p_source_id: id источника
    """
    l_error=None
    l_json_object=None
    l_id=None
    if p_id:
        l_id=[str(p_id)]
    l_attr=None
    if p_name or p_entity:
        l_attr={}
        if p_name:
            l_attr={C_NAME:p_name}
        if p_entity:
            # вытаскиваем id сущности
            l_entity_meta=search_object(p_type=C_ENTITY,p_attrs={C_NAME:p_entity})
            if l_entity_meta:
                l_entity=Entity(p_id=str(l_entity_meta[0].uuid))
                l_attr.update({C_ENTITY:l_entity.id})
            else:
                l_attr.update({C_ENTITY:C_DUMMY_UUID}) # если указано неправильное имя сущности - ничего не найдется
    l_ent_attr_meta=search_object(p_type=C_ENTITY_COLUMN, p_uuid=l_id, p_attrs=l_attr)
    if l_ent_attr_meta.__len__()==0:
        l_error="Ничего не найдено"
    else:
        l_json_object=[]
        for i_ent_attr in l_ent_attr_meta:
            l_ent_attr=Attribute(
                p_type=C_ENTITY_COLUMN,
                p_id=str(i_ent_attr.uuid)
            )
            for i_queue_column in l_ent_attr.source_attribute:
                if not p_source_id or i_queue_column.source_table.source.id==p_source_id: # если заданный id источника совпадает с id источника атрибута источника
                    l_ent_attr_dict={
                        C_ID:l_ent_attr.id,
                        C_NAME:l_ent_attr.name,
                        C_ENTITY:l_ent_attr.entity.name,
                        C_SOURCE_COLUMN:i_queue_column.name,
                        C_SOURCE_TABLE:i_queue_column.source_table.name,
                        C_SOURCE_NAME:i_queue_column.source_table.source.name,
                        C_SOURCE_ID:i_queue_column.source_table.source.id
                    }
                    l_json_object.append(
                        _JsonObject(p_type=C_ENTITY_COLUMN,p_id=str(l_ent_attr.id), p_attribute=l_ent_attr_dict)
                    )
    return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body

def get_entity_source(p_id: str =None, p_name: str =None, p_source_id: str =None):
    """
    Получение информации по источникам сущностей

    :param p_id: id сущности
    :param p_name: наименование сущности
    :param p_source_id: id источника
    """
    l_error=None
    l_json_object=None
    l_id=None
    if p_id:
        l_id=[str(p_id)]
    l_attr=None
    if p_name:
        l_attr={C_NAME:p_name}
    l_ent_meta=search_object(p_type=C_ENTITY, p_uuid=l_id, p_attrs=l_attr)
    if l_ent_meta.__len__()==0:
        l_error="Ничего не найдено"
    else:
        l_json_object=[]
        for i_ent in l_ent_meta:
            l_ent=Entity(
                p_id=str(i_ent.uuid)
            )
            for i_source in l_ent.source:
                if not p_source_id or i_source.id==p_source_id: # если заданный источник совпадает с источником сущности
                    l_ent_dict={
                        C_ID:l_ent.id,
                        C_NAME:l_ent.name,
                        C_SOURCE_NAME:i_source.name,
                        C_SOURCE_ID:i_source.id
                    }
                    l_json_object.append(
                        _JsonObject(p_type=C_ENTITY_COLUMN,p_id=str(l_ent.id), p_attribute=l_ent_dict)
                    )
    return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body

def start_job(p_entity: str =None, p_entity_attribute: str =None):
    """
    Запускает джоб

    :param p_entity: id сущности, который требуется прогрузить
    :param p_entity_attribute: id атрибута сущности, который требуется прогрузить
    """
    l_entity=None
    if p_entity:
        l_entity=Entity(
            p_id=p_entity
        )
    l_entity_attribute=None
    if p_entity_attribute:
        l_entity_attribute=Attribute(
            p_type=C_ENTITY_COLUMN,
            p_id=p_entity_attribute
        )
    l_job=Job(
        p_entity=l_entity,
        p_entity_attribute=l_entity_attribute
    )
    l_job.start_job()
    return _JsonOutput(p_json_object=None, p_message="Загрузка данных завершена").body



def add_entity(p_json: json):
    """
    Создает сущность в ХД

    :param p_json: json с параметрами сущности
    """

    l_model=Model(p_json=p_json)
    l_model.create_model()
    return _JsonOutput(p_json_object=None, p_message="Сущность успешно создана").body


class _JsonObject:
    """
    Объект в формате json
    """

    def __init__(self, p_id: str, p_type: str, p_attribute: dict):
        """
        Конструктор

        :param p_id: id объекта
        :param p_type: тип объекта
        :param p_attribute: атрибут объекта
        """
        self._id=p_id
        self._type=p_type
        self._attribute=p_attribute


    @property
    def body(self) -> dict:
        """
        Тело результата
        """
        l_dict={
            C_TYPE_VALUE:self._type,
            C_ID:self._id

        }
        l_attribute_key_list=list(self._attribute.keys())
        for i_key in l_attribute_key_list:
            l_dict.update(
                {
                    i_key:self._attribute.get(i_key)
                }
            )

        return l_dict

class _JsonOutput:
    """
    Результат в формате json
    :param p_message: текст сообщения
    """

    def __init__(self, p_json_object: list =None, p_message: str =None, p_error: str =None):
        """
        Конструктор

        :param p_json_object: лист объектов в формате json (объекты класса JsonObject)
        """
        self._json_object=p_json_object
        self._message=p_message
        self._error=p_error

    @property
    def body(self) -> json:
        """
        Тело результата
        """
        l_output={}
        if self._message:
            l_output.update({
                C_MESSAGE:self._message
            })
        if self._error:
            l_output.update({
                C_ERROR:self._error
            })
        if self._json_object and self._json_object.__len__()>0:
            l_object_list=[]
            for i_json_object in self._json_object:
                l_object_list.append(i_json_object.body)
            l_output.update({
                C_DATA:l_object_list
            })
        l_output=json.dumps(l_output)
        return l_output