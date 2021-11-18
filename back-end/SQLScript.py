import pyodbc
import sys
import datetime
import json
import Support
#SQL скрипты
class SQLScript:

    # скрипт отбора таблиц схемы meta
    @staticmethod
    def meta_objects_sql():
        scrpt = "select tablename from pg_catalog.pg_tables where schemaname='meta';"
        return scrpt
    # скрипт отбора метаданных
    @staticmethod
    def select_meta_sql(object, id=None):
        fltr=""
        if id is not None:
            fltr = " where id='"+str(id)+"'"
        scrpt = "select id, value from meta."+str(object)+fltr+";"
        return scrpt
    # скрипт проверки наличия id
    @staticmethod
    def exist_id_sql(object, id):
        scrpt="select count(*) from meta."+str(object)+" where id='"+str(id)+"';"
        return scrpt

    # скрипт обновления метаданных
    @staticmethod
    def update_meta_sql(object, id, object_attr):
        scrpt = "update meta."+str(object)+" set value='"+str(object_attr)+"' where id='"+str(id)+"';"
        return scrpt

    # скрипт вставки метеданных
    @staticmethod
    def insert_meta_sql(object, guid, object_attr):
        scrpt="insert into meta."+str(object)+" (id, value) values ('"+str(guid)+"','"+object_attr+"');"
        return scrpt

    # скрипт удаления по uuid из метаданных
    @staticmethod
    def deleted_uuid_sql(table, uuid):
        fltr=" where id in (null"
        for i in uuid:
            fltr=fltr+",'"+str(i)+"'"
        scrpt="delete from meta."+str(table)+fltr+");"
        return scrpt

    # скрипт вставляющий запись о транзакции
    @staticmethod
    def insert_tran_sql(tran):
        tran_id=tran.get("id",None)
        tran_json=json.dumps(tran)
        scrpt="insert into meta.transaction values ('"+str(tran_id)+"','"+str(tran_json)+"');"
        return scrpt



    @staticmethod
    def SourceId(source_id=None, disable_flg=None):

        select = "select " \
                 "distinct " \
                 "source_id " \
                 "from metadata.source "
        filter="where 1=1 "
        script=""

        if source_id is not None:
            filter=filter+" and source_id="+str(source_id)
        if disable_flg is not None:
            filter=filter+" and disable_flg='"+str(disable_flg)+"'"

        script= select + filter + ";"

        return script

    @staticmethod
    def LinkObjectId(object_type, object_id, link_object_type, reverse=None):
        #reverse - при 1, меняет местами object_type и link_object_type в наименовании таблицы
        if object_type == link_object_type:
            link_object_type_attr = "link_"+link_object_type
        else:
            link_object_type_attr = link_object_type

        if reverse == 1:
            table_name = object_type+"_"+link_object_type
        else:
            table_name = link_object_type+"_"+object_type

        script="select "+link_object_type_attr+"_id, "+object_type+"_id from metadata."+table_name+" where "+object_type+"_id"+"="+str(object_id)+";"

        return script


    @staticmethod
    def ObjectCheck(object_id, object_type):
        script = "select count(*) from metadata."+object_type+" where "+object_type+"_id="+str(object_id)+" and disable_flg='0';"
        return script

    @staticmethod
    def ObjectNameCheck(object_name, object_type, object_schema=None):

        obj_schm_script = ""
        if object_schema is not None:
            obj_schm_script = " and obj_schm.attr_value='"+object_schema+"'"
        script = "select count(*)" \
                 " from metadata."+object_type+" obj" \
                 " left join metadata."+object_type+"_attr obj_nm" \
                 " on obj."+object_type+"_id=obj_nm."+object_type+"_id" \
                 " and obj_nm.attr_type='"+object_type+"_name'" \
                 " left join metadata."+object_type+"_attr obj_schm" \
                 " on obj."+object_type+"_id=obj_schm."+object_type+"_id" \
                 " and obj_schm.attr_type='schema'" \
                 " where obj_nm.attr_value='"+object_name+"'"+obj_schm_script+" and obj.disable_flg=0;"
        return script

    @staticmethod
    def ObjectId(object_type, disable_flg=None,object_attr_type=None, object_attr_vl=None):

        select = "select " \
                 " obj."+object_type+"_id" \
                 " from metadata."+object_type+" obj " \
                 " left join metadata."+object_type+"_attr obj_attr " \
                 " on obj."+object_type+"_id=obj_attr."+object_type+"_id "
        filter=" where 1=1 "

        if disable_flg is not None:
            filter=filter+" and obj.disable_flg='"+str(disable_flg)+"'"
        if object_attr_type is not None and object_attr_vl is not None:
            filter = filter+" and obj_attr.attr_type='"+object_attr_type+"' and obj_attr.attr_value='"+object_attr_vl+"'"
        script=select + filter + ";"

        return script

    @staticmethod
    def ObjectAttr(object_type, object_id=None, disable_flg=None, object_attr_type=None):

        select= "select " \
                "obj."+object_type+"_id, " \
                "obj_attr.attr_type, " \
                "obj_attr.attr_value " \
                "from metadata."+object_type+" obj " \
                "left join metadata."+object_type+"_attr obj_attr " \
                "on obj."+object_type+"_id=obj_attr."+object_type+"_id "
        filter="where 1=1 "
        script=""

        if object_id is not None:
            filter=filter+" and obj."+object_type+"_id="+str(object_id)
        if disable_flg is not None:
            filter=filter+" and obj.disable_flg='"+str(disable_flg)+"'"
        if object_attr_type is not None:
            filter = filter+" and obj_attr.attr_type='"+object_attr_type+"'"

        script=select + filter + ";"

        return script

    @staticmethod
    def ChildObjectAttr(object_type, object_id, child_object_type):

        script = "select lnk."+child_object_type+"_id, chld_attr.attr_type, chld_attr.attr_value" \
                 " from metadata."+object_type+"_"+child_object_type+" as lnk" \
                 " left join metadata."+child_object_type+"_attr as chld_attr" \
                 " on 1=1 and lnk."+child_object_type+"_id=chld_attr."+child_object_type+"_id" \
                 " where 1=1 and lnk."+object_type+"_id="+str(object_id)
        return script

    @staticmethod
    def MaxObjectId(object_type):

        script = "select coalesce(max("+object_type+"_id),0) from metadata."+object_type+";"
        return script

    @staticmethod
    def InsertObject(object_type,object_id):

        script = "insert into metadata."+object_type+" ("+object_type+"_id, disable_flg) values ("+str(object_id)+",'0');"
        return script

    @staticmethod
    def InsertLinkObject(object1, object2,object1_id, object2_id):

        object_type = object1+"_"+object2
        if object1 == object2:
            object2 = "link_"+object2

        script = "insert into metadata."+object_type+" ("+object1+"_id, "+object2+"_id, disable_flg) values ("+str(object1_id)+","+str(object2_id)+",0);"
        return script

    @staticmethod
    def InsertObjectAttr(object_type, object_id, object_attr):

        insert = "insert into metadata."+object_type+"_attr ("+object_type+"_id, attr_type, attr_value) values "

        values = ""
        attr_key = list(object_attr.keys())
        source_attr_len = len(list(object_attr.keys()))
        i=0
        while i < source_attr_len:
            if i!=source_attr_len-1:
                comma = ","
            else: comma=""
            if object_attr[attr_key[i]] is None:
                object_attr[attr_key[i]] = "none"
            values = values +"("+str(object_id)+",'"+attr_key[i]+"','"+str(object_attr[attr_key[i]])+"')"+comma
            i = i + 1

        script = insert + values + ";"

        return script

    @staticmethod
    def DeleteObjectAttr(object_type, object_id, attr_type=None):

        attr_type_sql=""
        if attr_type is not None:
            attr_type_sql=" and attr_type='"+attr_type

        script = "delete from metadata."+object_type+"_attr where "+object_type+"_id="+str(object_id)+attr_type_sql+"';"
        return script

    @staticmethod
    def DeleteObject(object_type, object_id):

        script = "update metadata."+object_type+" set disable_flg='1' where "+object_type+"_id="+str(object_id)+";"
        return script

    @staticmethod
    def DeleteLinkObject(object1, object2, object1_id, object2_id):

        script = "update metadata."+object1+"_"+object2+" set disable_flg=1 where "+object1+"_id"+"="+str(object1_id)+" and "+object2+"_id"+"="+str(object2_id)+";"

        return script

    @staticmethod
    def DeleteAllLinkObject(object1, object2, object2_id):

        script = "update metadata."+object1+"_"+object2+" set disable_flg=1 where "+object2+"_id"+"="+str(object2_id)+";"

        return script

    @staticmethod
    def ListSourceObject(database=None, schema=None, table=None):

        select = "select " \
                 "tab.table_catalog, " \
                 "tab.table_schema, " \
                 "tab.table_name, " \
                 "tab.table_type, " \
                 "col.column_name, " \
                 "col.data_type, " \
                 "col.character_maximum_length, " \
                 "col.numeric_precision, " \
                 "col.numeric_scale, " \
                 "case when ky.constraint_name is not null then 1 else 0 end " \
                 "from information_schema.tables tab " \
                 "left join information_schema.columns col " \
                 "on 1=1 " \
                 "and tab.table_catalog=col.table_catalog " \
                 "and tab.table_schema=col.table_schema " \
                 "and tab.table_name=col.table_name " \
                 "left join information_schema.key_column_usage ky " \
                 "on 1=1 " \
                 "and tab.table_catalog=ky.table_catalog " \
                 "and tab.table_schema=ky.table_schema " \
                 "and tab.table_name=ky.table_name " \
                 "and col.column_name=ky.column_name " \
                 "and substring(ky.constraint_name,1,2)='PK'"
        filter = " where 1=1"
        if database is not None:
            filter = filter + " and tab.table_catalog='"+database+"'"
        if schema is not None:
            filter = filter + " and tab.table_schema='"+schema+"'"
        if table is not None:
            filter = filter + " and tab.table_name='"+table+"'"

        script = select + filter + ";"

        return script

    @staticmethod
    def TableRaw(database, schema, table):

        script = "select top 10 * from "+database+"."+schema+"."+table

        return script


    @staticmethod
    def SQLCreateTable(schema, table, source_schema, source_table,source, attr=None):
        # создает таблицу с помощью конструкции create table as select
        # берет все атрибуты (с помощью *) из таблицы источника
        # source_schema, source_table - схема и таблица источника, на основе которой будет создаваться таблица
        # attr - список дополнительных атрибутов, которые будут заполнены null
        #attr: {
        #           "column_name":"наименование атрибута"
        #           ,"datatype":"тип данных"
        #           ,"length":"Размер десятичного числа/строки"
        #           ,"scale":"Количество знаков после запятой"}]
        # source - указатель, что типы данных из сторонней СУБД (не PostgreSQL)

        attr_script = ""
        if attr is not None:
            attr_len = len(attr)
            attr_script = " , "
            i=0
            while i<attr_len:
                datatype_length = ""
                scale = ""
                datatype = ""
                comma = ","
                if source == 1:
                    datatype = Support.Support.DataTypeMapping(attr[i].get("datatype",None),attr[i].get("length",None))["datatype"]
                    datatype_length = Support.Support.DataTypeMapping(attr[i].get("datatype",None),attr[i].get("length",None))["length"]
                if attr[i].get("scale",None) is not None:
                    scale = ","+str(attr[i].get("scale",None))
                if datatype_length is not None:
                    datatype_length = "("+str(datatype_length)+scale+")"
                else:
                    datatype_length = ""
                if i == attr_len - 1:
                    comma=""
                attr_script = attr_script + "cast(null as "+datatype+datatype_length+") as "+'"'+attr[i].get("column_name",None)+'"'+comma
                i=i+1

        script = "create table "+schema+"."+'"'+table+'"'+" as ( select src.* "+attr_script+" from "+source_schema+"."+'"'+source_table+'"'+" as src);"

        return script


    @staticmethod
    def SelectData(source_table, source_schema,source_attr, increment_attr=None, increment_attr_vl=None, status_id=None):

        script_attr=""
        src_attr_len = len(source_attr)
        i = 0
        while i < src_attr_len:
            comma = ","
            if i == src_attr_len - 1:
                comma = ""
            script_attr = script_attr +'"'+source_attr[i]+'"'+ comma
            i=i+1
        script_where = ""
        if increment_attr is not None and increment_attr_vl is not None:
            script_where = " where 1=1 and "+'"'+increment_attr+'"'+">'"+str(increment_attr_vl)+"'"
        script_status = ""
        if status_id is not None:
            script_status = " where 1=1 and status_id='"+str(status_id)+"'"
        script = "select "+script_attr+" from "+'"'+source_schema+'"'+"."+'"'+source_table+'"'+script_where+script_status

        return script

    @staticmethod
    def InsertData(target_table,target_schema, target_attr, target_vl):
        # значения должны быть расположены в том же порядке, что и атрибуты
        # target_vl = [[],[]]
        script_attr = " ("
        target_attr_len = len(target_attr)
        i = 0
        while i < target_attr_len:
            comma = ","
            if i == target_attr_len - 1:
                comma = ""
            script_attr = script_attr + '"'+str(target_attr[i])+'"'+comma
            i=i+1
        script_attr = script_attr + ")"
        script_vl = ""
        target_vl_len = len(target_vl)
        j=0
        while j < target_vl_len:
            script_vl = script_vl + " ("
            target_vl_attr_len = len(target_vl[j])
            k=0
            while k < target_vl_attr_len:
                comma1 = ","
                if j == target_vl_len - 1:
                    comma1 = ""
                comma2 = ","
                if k == target_vl_attr_len - 1:
                    comma2 = ""
                if str(target_vl[j][k]) == "current_timestamp":
                    quote = ""
                else:
                    quote = "'"
                script_vl = script_vl + quote + str(target_vl[j][k]).replace("'","''")+quote+comma2
                k=k+1
            script_vl = script_vl + ")"+comma1
            j=j+1
        script = "insert into "+'"'+target_schema+'"'+"."+'"'+str(target_table)+'"'+script_attr+" values"+script_vl
        return script

    @staticmethod
    def RowCount(table_schema, table_name):
        script = "select count(*) from "+'"'+str(table_schema)+'"'+"."+'"'+str(table_name)+'"'
        return script

    @staticmethod
    def MaxETLId(table_schema, table_name):
        script = "select coalesce(max(etl_id),0) from "+'"'+str(table_schema)+'"'+"."+'"'+str(table_name)+'"'
        return script

    @staticmethod
    def DeleteDataETLId(table_schema, table_name, etl_id):
        script = "delete from "+'"'+str(table_schema)+'"'+"."+'"'+str(table_name)+'"'+" where etl_id="+str(etl_id)
        return script

    @staticmethod
    def InsertETLLog(etl_id, table_id, status_id, row_cnt):
        script = "insert into metadata.etl_log (etl_id, table_id, status_id, row_cnt, processed_dttm)" \
                 "values ("+str(etl_id)+","+str(table_id)+","+str(status_id)+","+str(row_cnt)+",current_timestamp)"
        return script

    @staticmethod
    def StageRowsToLoad(view_nm):
        script = "select count(*) from stg."+'"'+str(view_nm)+'"'+" where status_id=1"
        return script