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
        l_json_object.append(
            _JsonObject(p_id=i_source.id, p_type=C_SOURCE, p_attribute=i_source.source_meta_attrs)
        )
    return _JsonOutput(p_json_object=l_json_object).body



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
    """

    def __init__(self, p_json_object: list =None):
        """
        Конструктор

        :param p_json_object: лист объектов в формате json (объекты класса JsonObject)
        """
        self._json_object=p_json_object

    @property
    def body(self) -> json:
        """
        Тело результата
        """
        if not self._json_object or self._json_object.__len__()==0:
            l_output= {
                C_ERROR:"Ничего не найдено"
            }
        else:
            l_object_list=[]
            for i_json_object in self._json_object:
                l_object_list.append(i_json_object.body)
            l_output= {
                C_DATA:l_object_list
            }
        l_output=json.dumps(l_output)
        return l_output