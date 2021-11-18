import Metadata
import SQLScript
import Support
import MetadataETL
import SourceConnection
import pyodbc
import sys
import datetime
import json
#TODO: метод, который принимает на вход массив entity и вызывает методы маппинга бизнес-модели
#TODO: для проставления связи между сущностями - отдельный метод
class Entity:

    @staticmethod
    #создание сущности
    def CreateEntity(entity):
        # entity -
        # {
        #   "entity_name":"наименование сущности (на английском)",
        #   "entity_desc":"описание сущности или факта",
        #   "entity_attr":[{
        #                     "attribute_name":"наименование атрибута (на английском)",
        #                     "attribute_desc":"описание атрибута",
        #                     "source":[{
        #                                   "source_id":"id источника",
        #                                   "source_schema":"схема таблицы источника",
        #                                   "source_table":"наименование таблицы источника",
        #                                   "source_attribute_name":"наименование атрибута источника",
        #                                   "key":"признак ключа сущности",
        #                                   "foreign_key":"признак внешнего ключа",
        #                                   "foreign_entity":"наименование сущности, на которую ссылается foreign_key"
        #                               }]
        #                }]
        # }
        #
        result = {}
        creation = []
        error = []

        # добавляем сущность в метаданные
        entity_attr = {"entity_name":entity["entity_name"],"entity_desc":entity["entity_desc"]}
        ins_entity = MetadataETL.MetadataETL.InsertObject("entity",entity_attr)
        if ins_entity["result"][0].get("result",None) is None:
            return ins_entity
        else:
            entity_id = ins_entity["result"][0].get("object_id",None)
        # добавляем атрибуты сущности в метаданные
        entity_attribute_dict = []
        for ent_attr in entity["entity_attr"]:
            # определяем является ли атрибут ключом или внешним ключом
            key = 0
            f_key = 0
            f_name = ""
            for src_attr in ent_attr["source"]:
                if src_attr["key"] == "1":
                    key = key + 1
                if src_attr["foreign_key"] == "1":
                    f_key = f_key + 1
                    f_name = src_attr["foreign_entity"]
            entity_attribute_attr = {
                "attribute_name":ent_attr["attribute_name"]
                ,"attribute_desc":ent_attr["attribute_desc"]
            }
            if key > 0:
                 key = 1
                 entity_attribute_attr.update({"key":str(key)})
            if f_key > 0:
                f_key = 1
                entity_attribute_attr.update({"foreign_key":str(f_key),"foreign_entity":f_name})

            # связка атрибутов сущности с сущностью
            entity_attribute_lnk = [{"link_object_type":"entity","link_object_id":entity_id}]
            ins_attr_entity = MetadataETL.MetadataETL.InsertObject("entity_attribute",entity_attribute_attr, entity_attribute_lnk)
            if ins_attr_entity["result"][0].get("result",None) is None:
                return ins_attr_entity
            else:
                entity_attribute_id = ins_attr_entity["result"][0].get("object_id",None)

            entity_attribute_attr.update({"attribute_id":entity_attribute_id})
            entity_attribute_dict.append(entity_attribute_attr)


        creation = {"entity_id":entity_id,"entity_attr":entity_attribute_dict}
        error.clear()
        error.append({"error_flg":"0","error_code":None,"error_text":None})
        result.clear()
        result.update({"result":[{"creation":creation,"error":error}]})

        return result

    @staticmethod
    def AlterEntity(entity):
        # entity -
        # {
        #   "entity_id":"ID сущности"
        #   "entity_name":"наименование сущности (на английском)",
        #   "entity_desc":"описание сущности или факта",
        #   "entity_attr":[{
        #                     "change_type":"1 - добавление атрибута","изменение существуюшего атрибута","удаление атрибута"
        #                     "attribute_id":"id атрибута" если изменяется существующий атрибут
        #                     "attribute_name":"наименование атрибута (на английском)",
        #                     "attribute_desc":"описание атрибута",
        #                     "source":[{
        #                                   "source_id":"id источника",
        #                                   "source_schema":"схема таблицы источника",
        #                                   "source_table":"наименование таблицы источника",
        #                                   "source_attribute_name":"наименование атрибута источника",
        #                                   "key":"признак ключа сущности",
        #                                   "foreign_key":"признак внешнего ключа",
        #                                   "foreign_entity":"наименование сущности, на которую ссылается foreign_key"
        #                               }]
        #                }]
        # }
        # если у ключа словаря есть значение - данный атрибут сущности будет изменен
        result = {}
        alter = []
        error = []
        if entity.get("entity_id",None) is None:
            error.clear()
            error.append({"error_flg":"0","error_code":None,"error_text":"Entity id is not mentioned"})
            result.clear()
            result.update({"result":[{"result":None,"error":error}]})
            return result
        else:
            entity_id = entity["entity_id"]

        #изменяем наименование и описание сущности, если требуется
        alt_ent_attr = {}
        if entity.get("entity_name", None) is not None or entity.get("entity_desc",None) is not None:
            if entity.get("entity_name",None) is not None:
                alt_ent_attr.update({"entity_name":entity["entity_name"]})
            if entity.get("entity_desc",None) is not None:
                alt_ent_attr.update({"entity_desc":entity["entity_desc"]})
        alt_ent = MetadataETL.MetadataETL.UpdateObject("entity",entity_id,1,alt_ent_attr)
        if alt_ent["result"][0].get("result",None) is None:
            return alt_ent

        alt_ent_attr.clear()
        #изменение атрибутов сущности, если требуется
        if len(entity.get("entity_attr",{})) > 0:
            for ent_attr in entity["entity_attr"]:
                #добавление атрибута
                if ent_attr["change_type"]=="1":
                    ins_ent_attr ={
                        "attribute_name":ent_attr["attribute_name"]
                        ,"attribute_desc":ent_attr.get("attribute_desc",None)
                    }
                    entity_attribute_lnk = [{"link_object_type":"entity","link_object_id":entity_id}]
                    ins_attr = MetadataETL.MetadataETL.InsertObject("entity_attribute",ins_ent_attr,entity_attribute_lnk)
                    if ins_attr["result"][0].get("result",None) is None:
                        return ins_attr
                if ent_attr["change_type"] == "2":
                    if ent_attr.get("attribute_id",None) is None:
                        error.clear()
                        error.append({"error_flg":"0","error_code":None,"error_text":"Attribute id is not mentioned"})
                        result.clear()
                        result.update({"result":[{"result":None,"error":error}]})
                        return result
                    else:
                        attribute_id = ent_attr["attribute_id"]
                    alt_ent_attr = {}
                    if ent_attr.get("attribute_name",None) is not None:
                        alt_ent_attr.update({"attribute_name":ent_attr["attribute_name"]})
                    if ent_attr.get("attribute_desc",None) is not None:
                        alt_ent_attr.update({"attribute_desc":ent_attr["attribute_desc"]})
                    alt_ent = MetadataETL.MetadataETL.UpdateObject("entity_attribute",attribute_id,1,alt_ent_attr)
                    if alt_ent["result"][0].get("result",None) is None:
                        return alt_ent
                if ent_attr["change_type"] == "3":
                    if ent_attr.get("attribute_id",None) is None:
                        error.clear()
                        error.append({"error_flg":"0","error_code":None,"error_text":"Attribute id is not mentioned"})
                        result.clear()
                        result.update({"result":[{"result":None,"error":error}]})
                        return result
                    else:
                        attribute_id = ent_attr["attribute_id"]
                    del_attr = MetadataETL.MetadataETL.DeleteObject("entity_attribute",attribute_id)
                    if del_attr["result"][0].get("result",None) is None:
                        return del_attr

        alter = {"alter":"alter is successful"}
        error.clear()
        error.append({"error_flg":"0","error_code":None,"error_text":None})
        result.clear()
        result.update({"result":[{"result":alter,"error":error}]})

        return result

    @staticmethod
    def DeleteEntity(entity_id):
        #entity_id - id сущности

        result = {}
        delete = []
        error = []

        del_ent = MetadataETL.MetadataETL.DeleteObject("entity",entity_id)
        if del_ent["result"][0].get("result",None) is None:
            return del_ent
        else:
            delete = {"delete":"delete is successful"}
            error.clear()
            error.append({"error_flg":"0","error_code":None,"error_text":None})
            result.clear()
            result.update({"result":[{"result":delete,"error":error}]})

            return result


