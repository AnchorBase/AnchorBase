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

def get_last_etl():
    """
    Возвращает информацию о последнем ETL
    """
    l_error=None
    l_json_object=[]
    l_id=None
    # максимальный etl_id
    l_max_etl_id=get_max_etl_id()
    l_etl_meta_attrs={C_ETL_ATTRIBUTE_NAME:l_max_etl_id}
    l_last_etl_meta=search_object(p_type=C_ETL, p_attrs=l_etl_meta_attrs) # ищем в метаданных по etl_id
    if l_last_etl_meta.__len__()==0:
        l_error="Нет завершенных etl-процессов"
    else:
        l_etl=Job(
            p_id=str(l_last_etl_meta[0].uuid)
        )
        l_id=str(l_etl.id)
        l_entity_name=None
        if l_etl.entity:
            l_entity_name=l_etl.entity.name
        l_entity_attr_name=None
        if l_etl.entity_attribute:
            l_entity_attr_name=l_etl.entity_attribute.name
        l_etl_dict={
            C_ETL_ATTRIBUTE_NAME:l_etl.etl_id,
            C_ENTITY:l_entity_name,
            C_ENTITY_ATTRIBUTE:l_entity_attr_name,
            C_STATUS:l_etl.status,
            C_START_DATETIME:str(l_etl.start_datetime),
            C_END_DATETIME:str(l_etl.end_datetime),
            C_DURATION:l_etl.duration
        }
        l_json_object.append(
            _JsonObject(p_type=C_ETL,p_id=l_id, p_attribute=l_etl_dict)
        )
    return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body

def get_etl_hist(p_date: str =None):
    """
    Возвращает логи по etl-процессам

    :param p_date: дата запуска etl-процесса
    """
    l_error=None
    l_json_object=[]
    l_etl_meta=search_object(p_type=C_ETL)
    if l_etl_meta.__len__()==0:
        l_error="Нет завершенных etl-процессов"
    else:
        for i_etl_meta in l_etl_meta:
            l_etl=Job(
                p_id=str(i_etl_meta.uuid)
            )
            l_entity_name=None
            if l_etl.entity:
                l_entity_name=l_etl.entity.name
            l_entity_attr_name=None
            if l_etl.entity_attribute:
                l_entity_attr_name=l_etl.entity_attribute.name
            l_etl_dict={
                C_ETL_ATTRIBUTE_NAME:l_etl.etl_id,
                C_ENTITY:l_entity_name,
                C_ENTITY_ATTRIBUTE:l_entity_attr_name,
                C_STATUS:l_etl.status,
                C_START_DATETIME:str(l_etl.start_datetime),
                C_END_DATETIME:str(l_etl.end_datetime),
                C_DURATION:l_etl.duration
            }
            if not p_date or datetime.datetime.strptime(p_date,'%Y-%m-%d').date()==l_etl.start_datetime.date():
                l_json_object.append(
                    _JsonObject(p_type=C_ETL,p_id=l_etl.id, p_attribute=l_etl_dict)
                )
    return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body

def get_etl_detail(p_etl: str =None, p_etl_id: str =None):
    """
    Возвращает детализацию по etl-процессу

    :param p_etl: id etl
    :param p_etl_id: etl_id
    """
    l_error=None
    l_json_object=[]
    l_attr={}
    if not p_etl and not p_etl_id: # хоть одно должен быть заполнено
        l_error="Либо id, либо etl_id должны быть заполнены"
        return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body
    elif p_etl:
        l_attr.update(
            {C_ETL:p_etl}
        )
    elif p_etl_id:
        # ищем id etl
        l_etl_id=search_object(p_type=C_ETL, p_attrs={C_ETL_ATTRIBUTE_NAME:p_etl_id})
        if l_etl_id.__len__()>0:
            l_attr.update(
                {C_ETL:str(l_etl_id[0].uuid)}
            )
    else:
        l_attr=None
    # ищем логи в метаданных
    # логи по queue таблицам
    l_etl_queue_meta=search_object(
        p_type=C_QUEUE_ETL,
        p_attrs=l_attr
    )
    if l_etl_queue_meta.__len__()>0:
        for i_etl_queue in l_etl_queue_meta:
            l_queue_pack=Package(
                p_type=C_QUEUE_ETL,
                p_id=str(i_etl_queue.uuid)
            )
            l_queue_pack_dict={
                C_TABLE:l_queue_pack.source_table.queue_name,
                C_STATUS:l_queue_pack.status,
                C_ERROR:l_queue_pack.error,
                C_START_DATETIME:str(l_queue_pack.start_datetime),
                C_END_DATETIME:str(l_queue_pack.end_datetime),
                C_DURATION:l_queue_pack.duration
            }
            l_json_object.append(
                _JsonObject(p_type=C_QUEUE,p_id=l_queue_pack.id, p_attribute=l_queue_pack_dict)
            )
    # логи по idmap таблицам
    l_etl_idmap_meta=search_object(
        p_type=C_IDMAP_ETL,
        p_attrs=l_attr
    )
    if l_etl_idmap_meta.__len__()>0:
        for i_etl_idmap in l_etl_idmap_meta:
            l_idmap_pack=Package(
                p_type=C_IDMAP_ETL,
                p_id=str(i_etl_idmap.uuid)
            )
            l_idmap_pack_dict={
                C_TABLE:l_idmap_pack.idmap.name,
                C_STATUS:l_idmap_pack.status,
                C_ERROR:l_idmap_pack.error,
                C_START_DATETIME:str(l_idmap_pack.start_datetime),
                C_END_DATETIME:str(l_idmap_pack.end_datetime),
                C_DURATION:l_idmap_pack.duration
            }
            l_json_object.append(
                _JsonObject(p_type=C_IDMAP,p_id=l_idmap_pack.id, p_attribute=l_idmap_pack_dict)
            )
    # логи по якорным таблицам
    l_etl_anchor_meta=search_object(
        p_type=C_ANCHOR_ETL,
        p_attrs=l_attr
    )
    if l_etl_anchor_meta.__len__()>0:
        for i_etl_anchor in l_etl_anchor_meta:
            l_anchor_pack=Package(
                p_type=C_ANCHOR_ETL,
                p_id=str(i_etl_anchor.uuid)
            )
            l_anchor_pack_dict={
                C_TABLE:l_anchor_pack.anchor.name,
                C_STATUS:l_anchor_pack.status,
                C_ERROR:l_anchor_pack.error,
                C_START_DATETIME:str(l_anchor_pack.start_datetime),
                C_END_DATETIME:str(l_anchor_pack.end_datetime),
                C_DURATION:l_anchor_pack.duration
            }
            l_json_object.append(
                _JsonObject(p_type=C_ANCHOR,p_id=l_anchor_pack.id, p_attribute=l_anchor_pack_dict)
            )
    # логи по таблица атрибутам
    l_etl_attr_meta=search_object(
        p_type=C_ATTRIBUTE_ETL,
        p_attrs=l_attr
    )
    if l_etl_attr_meta.__len__()>0:
        for i_etl_attr in l_etl_attr_meta:
            l_attr_pack=Package(
                p_type=C_ATTRIBUTE_ETL,
                p_id=str(i_etl_attr.uuid)
            )
            l_attr_pack_dict={
                C_TABLE:l_attr_pack.attribute_table.name,
                C_STATUS:l_attr_pack.status,
                C_ERROR:l_attr_pack.error,
                C_START_DATETIME:str(l_attr_pack.start_datetime),
                C_END_DATETIME:str(l_attr_pack.end_datetime),
                C_DURATION:l_attr_pack.duration
            }
            l_json_object.append(
                _JsonObject(p_type=C_ATTRIBUTE,p_id=l_attr_pack.id, p_attribute=l_attr_pack_dict)
            )
    # логи по tie
    l_etl_tie_meta=search_object(
        p_type=C_TIE_ETL,
        p_attrs=l_attr
    )
    if l_etl_tie_meta.__len__()>0:
        for i_etl_tie in l_etl_tie_meta:
            l_tie_pack=Package(
                p_type=C_TIE_ETL,
                p_id=str(i_etl_tie.uuid)
            )
            l_tie_pack_dict={
                C_TABLE:l_tie_pack.tie.name,
                C_STATUS:l_tie_pack.status,
                C_ERROR:l_tie_pack.error,
                C_START_DATETIME:str(l_tie_pack.start_datetime),
                C_END_DATETIME:str(l_tie_pack.end_datetime),
                C_DURATION:l_tie_pack.duration
            }
            l_json_object.append(
                _JsonObject(p_type=C_TIE,p_id=l_tie_pack.id, p_attribute=l_tie_pack_dict)
            )
    return _JsonOutput(p_json_object=l_json_object, p_error=l_error).body

def add_entity(p_json: json):
    """
    Создает сущность в ХД

    :param p_json: json с параметрами сущности
    """

    l_model=Model(p_json=p_json)
    l_model.create_model()
    return _JsonOutput(p_json_object=None, p_message="Сущность успешно создана").body

def alter_entity(p_json: json):
    """
    Изменяет созданную ранее сущность

    :param p_json: json с указанием id и новыми параметрами сущности
    """
    l_message=""
    l_json=json.loads(p_json)
    l_model=Model(p_json=p_json)
    #  хотя бы один параметр для изменения должен быть задан
    if list(l_json.keys()).__len__()<2: # указан только id
        return _JsonOutput(p_json_object=None, p_error="Не указано что должно поменяться в сущности").body
    if l_json.get(C_ENTITY):
        l_model.rename_entity()
        l_message+="\nСущность успешно переименована"
    if l_json.get(C_DESC):
        l_model.alter_desc()
        l_message+="\nОписание сущности успешно изменено"
    l_message=l_message[1:]
    return _JsonOutput(p_json_object=None, p_message=l_message).body

def drop_entity(p_json: json):
    """
    Удаляет сущность

    :param p_json: json с указанием id сущности, которую нужно удалить
    """
    l_model=Model(p_json=p_json)
    l_model.drop_entity()
    return _JsonOutput(p_json_object=None, p_message="Сущность успешно удалена").body


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