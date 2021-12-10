# coding=utf-8
import psycopg2
import Postgresql as pgsql
import sys
from datetime import datetime
import json
import Support
import SQLScript
from SystemObjects import Constant as const
import metadata_config
import uuid
import copy
from settings.settings import C_POSTGRESQL_DATABASE, \
    C_POSTGRESQL_HOST, C_POSTGRESQL_PASSWORD, \
    C_POSTGRESQL_PORT, C_POSTGRESQL_USER

#Работа с метаданными
class Metadata:
    # TODO:устаревший класс
    # выполняет SQL-запросы к метаданным и возвращает результат запроса
    @staticmethod
    def meta_sql_exec(sql, noresult=None):
        # noresult = 1, когда не требуется возвращать результат запроса. По умолчанию = 0
        server = "localhost"
        database = "test"
        user = "pavelhamrin"
        password = ""
        port = 5433
        cnct = psycopg2.connect(
            dbname=database, 
            user=user,
            password=password, 
            host=server,
            port=port
        )
        cnct.autocommit = False
        crsr = cnct.cursor()
        try:
            crsr.execute(sql)
            if noresult == 0 or noresult is None:
                query_output = crsr.fetchall()
            else:
                query_output=""
            cnct.commit()
        except psycopg2.Error as e:
            error = Support.Support.error_output("Metadata","meta_sql_exec",e)
            cnct.rollback()
            sys.exit(error)
        finally:
            crsr.close()
            cnct.close()
        return query_output


    # проверка, что таблица в метаданных существует
    @staticmethod
    def meta_object_check(object):
        meta_obj_sql = SQLScript.SQLScript.meta_objects_sql()
        meta_obj = Metadata.meta_sql_exec(meta_obj_sql)
        for meta_obj in meta_obj:
            if object == meta_obj[0]:
                return True
        return False
    # проверка, что указанный id объекта метаданных существует
    @staticmethod
    def id_object_check(object, id):
        id_obj_sql = SQLScript.SQLScript.exist_id_sql(object,id)
        meta_obj = Metadata.meta_sql_exec(id_obj_sql)
        if meta_obj[0][0]>=1:
            return True
        return False

    # формирует guid
    @staticmethod
    def generate_guid():
        guid_sql="select uuid_generate_v4();"
        guid = Metadata.meta_sql_exec(guid_sql)
        return guid[0][0]

    # удаляем все строки с указанным uuid из всех таблиц метаданных
    @staticmethod
    def delete_uuid(uuid):
        meta_obj_sql = SQLScript.SQLScript.meta_objects_sql()
        meta_obj = Metadata.meta_sql_exec(meta_obj_sql)
        for meta_obj in meta_obj:
            del_sql=SQLScript.SQLScript.deleted_uuid_sql(meta_obj[0],uuid)
            Metadata.meta_sql_exec(del_sql,1)


    # выбирает метаданные
    @staticmethod
    def select_meta(object, id=None, deleted=None, object_attr=None):
        obj_list = []
        # проверяем объект на наличие
        obj_check = Metadata.meta_object_check(object)
        if obj_check == False:
            error = Support.Support.error_output("Metadata","select_meta","Object '"+str(object)+"' hasn't found in metadata")
            sys.exit(error)
        # вытаскиваем метаданные
        meta_sql = SQLScript.SQLScript.select_meta_sql(object, id)
        meta_obj = Metadata.meta_sql_exec(meta_sql)
        for mt_obj in meta_obj:
            i=0
            if deleted is not None:
                if int(mt_obj[1].get("deleted",0))!=int(deleted):
                    i=i+1
            if object_attr is not None:
                obj_attr=object_attr.items()
                obj_attr=list(obj_attr)
                for obj_attr in obj_attr:
                    if mt_obj[1].get(obj_attr[0],None)!=obj_attr[1]:
                        i=i+1

            if i>0:
                continue
            else:
                obj_list.append(mt_obj[1])
            mt_obj[1].update({"id": mt_obj[0]})
        return obj_list
    # выбирает метаданные родительского объекта
    @staticmethod
    def parent_select_meta(object, child_object, child_id, deleted=None):
        obj_list = []
        # проверяем объекты на наличие
        obj_check = Metadata.meta_object_check(object)
        if obj_check == False:
            error = Support.Support.error_output("Metadata","parent_select_meta","Object '"+str(object)+"' hasn't found in metadata")
            sys.exit(error)
        obj_check = Metadata.meta_object_check(child_object)
        if obj_check == False:
            error = Support.Support.error_output("Metadata","parent_select_meta","Object '"+str(child_object)+"' hasn't found in metadata")
            sys.exit(error)
        # вытаскиваем метаданные родительского объекта
        object_attr=Metadata.select_meta(object,None,deleted)
        # отбираем нужный родительский объект
        for a in object_attr:
            for b in a.get(child_object,None):
                    if str(child_id)==str(b):
                        return a


    # вставка транзакции
    @staticmethod
    def insert_tran(tran):
        uuid=Metadata.generate_guid()
        dttm=str(datetime.now())
        attr={"timestamp":dttm,"commit":False,"id":uuid}
        tran.update(attr)
        ins_tran_sql=SQLScript.SQLScript.insert_tran_sql(tran)
        Metadata.meta_sql_exec(ins_tran_sql, 1)
        return tran

    # удаление незакомиченных транзакций и объекты, созданные по ним
    @staticmethod
    def delete_tran():
        # определяем незакомиченные транзакции
        del_tran=Metadata.select_meta("transaction",None,None,{"commit":False})
        # удаляем транзакции по id
        del_uuid=[]
        for i in del_tran:
            del_uuid.append(i.get("id",None))
            # определяем объекты, созданные по данным транзакциям
            meta_obj_sql = SQLScript.SQLScript.meta_objects_sql() # определяем все объекты метаданных
            meta_obj = Metadata.meta_sql_exec(meta_obj_sql)
            for meta_obj in meta_obj:
                obj=Metadata.select_meta(meta_obj[0],None,None,{"transaction":[i.get("id",None)]})
                for j in obj:
                    # удаляем такие объекты
                    Metadata.delete_uuid([j.get("id",None)])
        # удаляем транзакцию
        Metadata.delete_uuid(del_uuid)

    # вставляет метаданные
    @staticmethod
    def insert_meta(object, object_attr, tran):
        # object_attr - {"object_attr_name":"object_attr_value"}
        # проверяем объект на наличие
        obj_check = Metadata.meta_object_check(object)
        if obj_check == False:
            error = Support.Support.error_output("Metadata","insert_meta","Object '"+str(object)+"' hasn't found in metadata")
            sys.exit(error)
        # проверяем на наличие атрибутов
        if object_attr is None:
            error = Support.Support.error_output("Metadata","insert_meta","There are no object attribute to insert")
            sys.exit(error)
        # формируем guid
        guid = Metadata.generate_guid()
        # вставляем признак неудаленного объекта
        if object_attr.get("deleted",None) is None:
            object_attr.update({"deleted":0})
        # вставляем транзакцию
        object_attr.update({"transaction":[tran.get("id",None)]})
        # вставляем метаданные
        object_attr = json.dumps(object_attr)
        insert_sql=SQLScript.SQLScript.insert_meta_sql(object, guid, object_attr)
        Metadata.meta_sql_exec(insert_sql, 1)
        object_attr = json.loads(object_attr)
        object_attr.update({"id":guid})
        return object_attr

    @staticmethod
    def update_meta(object, id, object_attr, tran):
        # обновляются только указанные в object_attr атрибуты
        # проверяем объект на наличие
        obj_check = Metadata.meta_object_check(object)
        if obj_check == False:
            error = Support.Support.error_output("Metadata","update_meta","Object '"+str(object)+"' hasn't found in metadata")
            sys.exit(error)
        # проверяем на наличие атрибутов
        if object_attr is None:
            error = Support.Support.error_output("Metadata","update_meta","There are no object attribute to insert")
            sys.exit(error)
        # проверяем на наличие id
        id_check = Metadata.id_object_check(object, id)
        if id_check == False:
            error = Support.Support.error_output("Metadata","update_meta","Object ID'"+str(object)+"' hasn't found in metadata")
            sys.exit(error)
        # достаем атрибуты
        cur_object_attr = Metadata.select_meta(object, id)[0]
        # вставляем транзакцию
        cur_object_attr["transaction"].append(tran.get("id",None))
        # обновляем атрибуты
        cur_object_attr.update(object_attr)
        cur_object_attr = json.dumps(cur_object_attr)
        update_sql=SQLScript.SQLScript.update_meta_sql(object, id, cur_object_attr)
        Metadata.meta_sql_exec(update_sql, 1)

        return json.loads(cur_object_attr)

    # собирает объект - доходит до всех дочерних объектов
    @staticmethod
    def object_meta(object, id=None, deleted=None):
         build = []
         # проверяем объект на наличие
         obj_check = Metadata.meta_object_check(object)
         if obj_check == False:
             error = Support.Support.error_output("Metadata","update_meta","Object '"+str(object)+"' hasn't found in metadata")
             sys.exit(error)
         # вытаскиваем объект и его атрибуты
         object_meta = Metadata.select_meta(object, id, deleted)
         # TODO: По-хорошему нужна рекурсия. Сейчас тупо реализованы вложенные циклы
         for objct_meta in object_meta:
             build.append(objct_meta)
             objct_attr=list(objct_meta.items())
             for objct_attr in objct_attr:
                 object_attr_type=objct_attr[0]
                 object_attr_vl=objct_attr[1]
                 if type(object_attr_vl) is list:
                     child1_list=[]
                     for i in object_attr_vl:
                         child1 = Metadata.select_meta(object_attr_type, i, deleted)
                         child1_list.append(child1[0])
                         child1_obj_attr=list(child1[0].items())
                         for child1_obj_attr in child1_obj_attr:
                             child1_obj_attr_type=child1_obj_attr[0]
                             child1_obj_attr_vl=child1_obj_attr[1]
                             if type(child1_obj_attr_vl) is list:
                                 child2_list=[]
                                 for j in child1_obj_attr_vl:
                                     child2 = Metadata.select_meta(child1_obj_attr_type, j, deleted)
                                     child2_list.append(child2[0])
                                     child2_obj_attr=list(child2[0].items())
                                     for child2_obj_attr in child2_obj_attr:
                                         child2_obj_attr_type=child2_obj_attr[0]
                                         child2_obj_attr_vl=child2_obj_attr[1]
                                         if type(child2_obj_attr_vl) is list:
                                             child3_list=[]
                                             for w in child2_obj_attr_vl:
                                                 child3 = Metadata.select_meta(child2_obj_attr_type, w, deleted)
                                                 child3_list.append(child3[0])
                                                 child3_obj_attr=list(child3[0].items())
                                                 for child3_obj_attr in child3_obj_attr:
                                                     child3_obj_attr_type=child3_obj_attr[0]
                                                     child3_obj_attr_vl=child3_obj_attr[1]
                                                     if type(child3_obj_attr_vl) is list:
                                                         child4_list=[]
                                                         for l in child3_obj_attr_vl:
                                                             child4 = Metadata.select_meta(child3_obj_attr_type, l, deleted)
                                                             child4_list.append(child4[0])
                                                         child3_obj_attr_vl.clear()
                                                         child3_obj_attr_vl.extend(list(child4_list))
                                                         child4_list.clear()
                                             child2_obj_attr_vl.clear()
                                             child2_obj_attr_vl.extend(list(child3_list))
                                             child3_list.clear()
                                 child1_obj_attr_vl.clear()
                                 child1_obj_attr_vl.extend(list(child2_list))
                                 child2_list.clear()
                     object_attr_vl.clear()
                     object_attr_vl.extend(list(child1_list))
                     child1_list.clear()
         return object_meta

    # возвращает id статуса по value
    @staticmethod
    def select_status(code):
        status_id=Metadata.select_meta("status",None,None,{"code":code})[0]
        return status_id.get("id")

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
        sys.exit(l_result[1])
    else:
        return l_result[0]

def __search_uuid_sql(p_uuid_list: list):
    """
    Формирует запрос поиска по метаданным с помощью uuid

    :param p_uuid_list: список uuid объекта/объектов метаданных
    """
    l_sql=" AND id IN ("
    for i_uuid in p_uuid_list:
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
    l_sql='INSERT INTO "'+p_type+'"'+" (id, value) VALUES \n('"+str(p_uuid)+"','"+l_attrs+"');"
    return l_sql

def __update_object_sql(p_type: str, p_uuid: str, p_attrs: dict):
    """
    Формирует запрос изменения данных
    :param p_type: тип объекта метаданных
    :param p_uuid: uuid объекта метаданных
    :param p_attrs: атрибуты объекта метаданных
    """
    l_attrs=json.dumps(p_attrs) # преобразуем в json строку
    l_sql='UPDATE "'+p_type+'"'+"\nSET value='"+l_attrs+"'\nWHERE id='"+str(p_uuid)+"';"
    return l_sql

def __delete_object_sql(p_type: str, p_uuid: str):
    """
    Формирует запрос удаления из метаданных

    :param p_type: тип объекта метаданных
    :param p_uuid: uuid объекта
    """
    l_sql='DELETE FROM "'+p_type+'"'+" WHERE ID='"+str(p_uuid)+"';"
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
    if l_type not in const('C_META_TABLES').constant_value:
        sys.exit("Нет объекта метаданных "+l_type) #TODO: переделать
    # фомируем SELECT
    l_sql='SELECT * FROM "'+l_type+'"'+" WHERE 1=1"
    # добавляем условия фильтрации
    l_where=""
    # фильтрация по id
    if p_uuid is not None and p_uuid.count(None)==0:
        l_where= l_where + __search_uuid_sql(p_uuid)
    elif p_uuid is not None and p_uuid.count(None)>0:
        sys.exit("Пустое значение uuid")
    # фильтрация по attr
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
        sys.exit("Объект не является объектом класса MetaObject") #TODO: переделать
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
        sys.exit("Объект не является объектом класса MetaObject") #TODO: переделать
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
        sys.exit("Объект не является объектом класса MetaObject")
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
                 p_attrs: dict,
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
        if l_meta_object not in const('C_META_TABLES').constant_value:
            sys.exit("Нет объекта метаданных "+l_meta_object) #TODO: переделать
        return l_meta_object

    @property
    def uuid(self) -> str:
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
        l_all_attrs_dict=const('C_META_ATTRIBUTES').constant_value.get(self.type,None) # достаем атрибуты объекта и их свойства
        l_all_attrs_list=list(l_all_attrs_dict.keys()) # список всех атрибутов объекта
        l_attr_list = list(self._attrs.keys()) # список указанных атрибутов объекта
        l_necessary_attrs_list=[]# необходимые атрибуты объекта
        # проверка, что все необходимые атрибуты указаны
        for i_attr in l_all_attrs_list: # собираем список необходимых атрибутов объекта
            if l_all_attrs_dict.get(i_attr,None).get(const('C_NOT_NULL').constant_value,None)==1:
                l_necessary_attrs_list.append(i_attr)
        for i_attr in l_necessary_attrs_list:
            if i_attr not in l_attr_list or self._attrs.get(i_attr,None) is None:
                sys.exit("Не указан атрибут "+i_attr)
        for i_attr in l_attr_list:
            # проверка, что атрибут указан верно
            if i_attr not in l_all_attrs_list:
                sys.exit("Некорректный атрибут "+str(i_attr))
            # проверка, что тип данных у атрибутов указан верно
            if type(self._attrs.get(i_attr,None)).__name__!=l_all_attrs_dict.get(i_attr,None).get(const('C_TYPE_VALUE').constant_value) and self._attrs.get(i_attr,None) is not None:
                sys.exit("Значение атрибута "+str(i_attr)+" некорректное")
            # проверка на дубль значения атрибута (только у ключевых атрибутов метаданных)
            if l_all_attrs_dict.get(i_attr, None).get(const('C_PK').constant_value)==1: # если атрибут ключевой
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
                    sys.exit("Уже присутствует объект "+self.type+" с "+i_attr+"="+str(self._attrs.get(i_attr,None))) #TODO: переделать















