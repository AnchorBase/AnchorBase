import Metadata
import SQLScript
import MSSQL
import Support
import pyodbc
import sys
import datetime
import json
import psycopg2
import Driver

# класс по работе с источником
class Source:

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
