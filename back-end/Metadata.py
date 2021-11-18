# coding=utf-8
import psycopg2
import sys
from datetime import datetime
import json
import Support
import SQLScript
from settings.settings import C_POSTGRESQL_DATABASE, \
    C_POSTGRESQL_HOST, C_POSTGRESQL_PASSWORD, \
    C_POSTGRESQL_PORT, C_POSTGRESQL_USER

#Работа с метаданными
class Metadata:

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

# print(json.dumps(Metadata.object_meta("queue_table")))
#




