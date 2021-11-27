import Metadata
import SQLScript
import Support
import Metadata
import Source
import Driver
import pyodbc
import sys
import datetime
import json
import copy
import hashlib
from SystemObjects import Constant as const


# класс по работе с DDL DWH (устаревший)
class DDL:
    #TODO: для update_dttm и атрибута таблицы attribute сделать проверки по типам данных
    #TODO: вынести проверки в отдельный метод отдельного класса
    # Устаревший класс

    @staticmethod
    def create_stage_table(name,attr,source_schema,tran,source_id):
        #attr: {
        #            "name":"наименование атрибута"
        #           ,"datatype":"тип данных"
        #           ,"length":"Размер десятичного числа/строки"
        #           ,"scale":"Количество знаков после запятой"}]
        # table_type: queue - таблица очереди, storage - таблица обработанных данных
        # наименования атрибутов таблицы источника и stage должны совпадать
        queue_attr=[]
        queue_attr = copy.deepcopy(attr)
        source_table = name # наименование должно совпадать
        # вытаскиваем наименование источника
        src_nm=Metadata.Metadata.select_meta("source",source_id)[0].get("name",None)
        queue_name=str(src_nm)+"_"+str(source_schema)+"_"+str(name)+"_queue"
        storage_name=str(src_nm)+"_"+str(source_schema)+"_"+str(name)+"_"+"_storage"
        src_type=Metadata.Metadata.select_meta("source",source_id)[0].get("type",None)
        # добавляем технические атрибуты
        for ar in queue_attr:
            if ar["name"] in ["deleted_flg","update_dttm","processed_dttm","status_id","row_id","check_flg"]:
                queue_attr.remove(ar)
            ar.pop("entity_column_key",None)
        queue_attr.extend(
            [
                {"name":"deleted_flg","datatype":"int","length":None,"scale":None,"type":"tech"},
                {"name":"update_dttm","datatype":Driver.Driver.convert_to_timestamp(),"length":None,"scale":None,"type":"tech"},
                {"name":"processed_dttm","datatype":Driver.Driver.convert_to_timestamp(),"length":None,"scale":None,"type":"tech"},
                {"name":"status_id","datatype":Driver.Driver.convert_to_guid(),"length":None,"scale":None,"type":"tech"},
                {"name":"row_id","datatype":Driver.Driver.convert_to_guid(),"length":None,"scale":None,"type":"tech"},
                {"name":"etl_id","datatype":Driver.Driver.convert_to_guid(),"length":None,"scale":None,"type":"tech"}
            ]
        )
        # проверки
        if source_id is None:
            error = Support.Support.error_output("DDL","create_stage_table","Creation of stage table needs source_id")
            sys.exit(error)
        # вытаскиваем 10 строк таблицы источника, чтобы проверить ее наличие
        Source.Source.source_raw(source_id,{"schema":source_schema,"table":source_table})
        for i in queue_attr:
            # проверяем, что все параметры у атрибутов указаны
            if "name" not in list(i.keys()):
                error = Support.Support.error_output("DDL","create_stage_table","Attribute parameter 'name' is not mentioned")
                sys.exit(error)
            elif "datatype" not in list(i.keys()):
                error = Support.Support.error_output("DDL","create_stage_table","Attribute parameter 'datatype' is not mentioned")
                sys.exit(error)
            elif "length" not in list(i.keys()):
                error = Support.Support.error_output("DDL","create_stage_table","Attribute parameter 'length' is not mentioned")
                sys.exit(error)
            elif "scale" not in list(i.keys()):
                error = Support.Support.error_output("DDL","create_stage_table","Attribute parameter 'scale' is not mentioned")
                sys.exit(error)
        # проверяем, что таблица источник еще не грузится в stage
        exist_src_tbls=Metadata.Metadata.select_meta("source_table",None,0,{"name":source_table,"schema":source_schema,"source":source_id})
        if len(exist_src_tbls)>0:
            error = Support.Support.error_output("DDL","create_stage_table","Source table '"+str(source_table)+"' has already loaded to stage")
            sys.exit(error)
        # проверяем на наличие stage таблицы
        exist_stg_tbls=Metadata.Metadata.select_meta("queue_table",None,0,{"name":queue_name})
        if len(exist_stg_tbls)>0:
            error = Support.Support.error_output("DDL","create_stage_table","Stage table '"+str(queue_name)+"' has been already created")
            sys.exit(error)
        # маппинг типов данных атрибутов источника на атрибуты stage
        for y in queue_attr:
            if y.get("type","0")!="tech":
                cor_datatype=Driver.Driver.data_type_map(y.get("datatype"),src_type,y.get("length",None))
                if cor_datatype is not None:
                    y.update({"datatype":cor_datatype.get("to_datatype",None)})
                    if cor_datatype.get("to_length",None) is not None:
                        y.update({"length":cor_datatype.get("to_length",None)})
        # добавление метаданных
        # атрибуты источника
        src_attr_uuid=[]
        ins_src_attr_list=[]
        for j in queue_attr:
            if j.get("type","0")!="tech":
                ins_src_attr=Metadata.Metadata.insert_meta("source_column",j, tran)
                ins_src_attr_list.append(ins_src_attr)
                src_attr_uuid.append(ins_src_attr.get("id",None))
                j.update({"source_column":[ins_src_attr.get("id",None)]})

        # таблица источника
        src_tbl_uuid=""
        src_tbl_attr={
            "name":source_table,
            "schema":source_schema,
            "source":source_id,
            "source_column":src_attr_uuid,
            "deleted":0
        }
        ins_src_tbl=Metadata.Metadata.insert_meta("source_table",src_tbl_attr, tran)
        src_tbl_uuid=ins_src_tbl.get("id",None)
        # атрибуты stage queue
        queue_attr_uuid=[]
        update_clmn_uuid=""
        for w in queue_attr:
            ins_queue_attr=Metadata.Metadata.insert_meta("queue_column",w,tran)
            w.update({"id":ins_queue_attr.get("id",None)})
            queue_attr_uuid.append(ins_queue_attr.get("id",None))
            for x in attr:
                if x.get("name")==w.get("name"):
                    x.update({"queue_column":ins_queue_attr.get("id",None)})
            if w.get("name",None)=="update_dttm":
                update_clmn_uuid=ins_queue_attr.get("id",None)
        # атрибуты stage storage
        storage_attr=copy.deepcopy(queue_attr)
        storage_attr_uuid=[]
        for y in storage_attr:
            ins_storage_attr=Metadata.Metadata.insert_meta("storage_column",y,tran)
            y.update({"id":ins_storage_attr.get("id",None)})
            storage_attr_uuid.append(ins_storage_attr.get("id",None))
            for z in attr:
                if z.get("name")==y.get("name"):
                    z.update({"storage_column":ins_storage_attr.get("id",None)})
        # таблица stage queue
        queue_tbl_attr={
            "name":queue_name,
            "type":"queue",
            "queue_column":queue_attr_uuid,
            "source_table":src_tbl_uuid,
            "deleted":0
        }
        ins_queue_tbl=Metadata.Metadata.insert_meta("queue_table",queue_tbl_attr,tran)
        db_queue_name=ins_queue_tbl.get("id",None)
        # таблица stage storage
        storage_tbl_attr={
            "name":storage_name,
            "type":"storage",
            "storage_column":storage_attr_uuid,
            "deleted":0
        }
        ins_storage_tbl=Metadata.Metadata.insert_meta("storage_table",storage_tbl_attr,tran)
        db_storage_name=ins_storage_tbl.get("id",None)
        # создание скрипта DDL
        create_queue_table_sql=Driver.Driver.create_table_ddl("stg",db_queue_name,queue_attr)
        create_queue_view_sql=Driver.Driver.create_view_sql("stg",queue_name,"stg",db_queue_name,queue_attr)
        create_storage_table_sql=Driver.Driver.create_table_ddl("stg",db_storage_name,storage_attr)
        create_storage_view_sql=Driver.Driver.create_view_sql("stg",storage_name,"stg",db_storage_name,storage_attr)
        create_sql=create_queue_table_sql+create_queue_view_sql#+create_storage_table_sql+create_storage_view_sql пока не создаем storage
        return {"sql":create_sql,"queue_table":db_queue_name,"storage_table":db_storage_name,"update_column":update_clmn_uuid}

    # создает скрипт DDL для IDMAP
    @staticmethod
    def create_idmap_ddl(name, queue_column_uuid,tran):
        # name - наименование сущности для которой создается IDMAP
        # queue_column_uuid - список id атрибутов таблицы queue, по которым будут генерироваться суррогаты
        name=str(name).lower()
        db_name="idmap_"+name
        # проверки
        # проверка на наличие idmap
        exist_idmap=Metadata.Metadata.select_meta("idmap",None,0,{"name":db_name})
        if len(exist_idmap)>0:
            error = Support.Support.error_output("DDL","create_idmap_ddl","Idmap '"+str(db_name)+"' already exists")
            sys.exit(error)
        # проверка на корректность указанных uuid queue_column
        for i in queue_column_uuid:
            uuid_cnt=Metadata.Metadata.select_meta("queue_column",i,0)
            if len(uuid_cnt)==0:
                error = Support.Support.error_output("DDL","create_idmap_ddl","Queue column '"+str(i)+"' hasn't found in metadata")
                sys.exit(error)
        # добавление метаданных
        # атрибуты таблицы idmap
        idmap_attr_rk={"name":name+"_rk","type":"rk","datatype":Driver.Driver.convert_to_guid(),"deleted":0}
        idmap_attr_nk={"name":name+"_nk","type":"nk","datatype":"varchar","length":1000,"queue_column":queue_column_uuid,"deleted":0}
        ins_idmap_attr_rk=Metadata.Metadata.insert_meta("idmap_column",idmap_attr_rk,tran)
        idmap_attr_rk.update({"id":ins_idmap_attr_rk.get("id",None)})
        ins_idmap_attr_nk=Metadata.Metadata.insert_meta("idmap_column",idmap_attr_nk,tran)
        idmap_attr_nk.update({"id":ins_idmap_attr_nk.get("id",None)})
        # таблица idmap
        idmap={"name":db_name,"idmap_column":[idmap_attr_nk.get("id",None),idmap_attr_rk.get("id",None)],"deleted":0}
        ins_idmap=Metadata.Metadata.insert_meta("idmap",idmap,tran)
        idmap.update({"id":ins_idmap.get("id",None)})
        # создание скрипта DDL
        create_idmap_sql=Driver.Driver.create_table_ddl("idmap",idmap.get("id",None),[idmap_attr_rk,idmap_attr_nk])
        create_idmap_view_sql=Driver.Driver.create_view_sql("idmap",db_name,"idmap",idmap.get("id",None),[idmap_attr_rk,idmap_attr_nk])
        create_idmap_ddl=create_idmap_sql+create_idmap_view_sql
        return {"sql":create_idmap_ddl,"idmap":idmap.get("id",None),"idmap_rk":idmap_attr_rk.get("id",None)}

    # генерация скрипта ddl anchor
    @staticmethod
    def create_anchor_table(name, idmap_rk_uuid, tran):
        # name - наименование сущности для которой создается Anchor
        # idmap_rk_uuid - суррогат из idmap
        name=str(name).lower()
        db_name=name+"_anch"
        # проверки
        # проверяем, что такого anchor не существует
        exist_anch=Metadata.Metadata.select_meta("anchor_table",None,0,{"name":db_name})
        if len(exist_anch)>0:
            error = Support.Support.error_output("DDL","create_anchor_table","Anchor '"+str(db_name)+"' already exists")
            sys.exit(error)
        # проверяем, что указанный uuid атрибута idmap существует
        exist_idmap_attr=Metadata.Metadata.select_meta("idmap_column",idmap_rk_uuid,0)
        if len(exist_idmap_attr)==0:
            error = Support.Support.error_output("DDL","create_anchor_table","Idmap column '"+str(idmap_rk_uuid)+"' hasn't found in metadata")
            sys.exit(error)
        # вставляем метаданные
        # атрибуты якоря
        anchor_attr=[
            {"name":str(name)+"_rk","datatype":Driver.Driver.convert_to_guid(),"type":"pk","idmap_column":[idmap_rk_uuid],"deleted":0},
            {"name":"source_system_id","datatype":Driver.Driver.convert_to_guid(),"type":"tech","deleted":0},
            {"name":"deleted_flg","datatype":"int","type":"tech","deleted":0},
            {"name":"processed_dttm","datatype":Driver.Driver.convert_to_timestamp(),"type":"tech","deleted":0}

        ]
        attr_uuid=[]
        for i in anchor_attr:
            ins_anchor_attr=Metadata.Metadata.insert_meta("anchor_column",i,tran)
            i.update({"id":ins_anchor_attr.get("id",None)})
            attr_uuid.append(ins_anchor_attr.get("id",None))
        # таблица
        anchor={"name":db_name,"type":"anchor","anchor_column":attr_uuid,"deleted":0}
        ins_anchor=Metadata.Metadata.insert_meta("anchor_table",anchor,tran)
        # генерация скрипта ddl
        create_table_sql=Driver.Driver.create_table_ddl("anch",ins_anchor.get("id",None),anchor_attr)
        create_view_sql=Driver.Driver.create_view_sql("anch",db_name,"anch",ins_anchor.get("id",None),anchor_attr)
        create_sql=create_table_sql+create_view_sql
        return {"sql":create_sql,"anchor_table":ins_anchor.get("id",None),"anchor_column":anchor_attr}

    # генерация скрипта создания таблицы attribute
    @staticmethod
    def create_attribute_table(name,attr,tran):
        # name - наименование сущности для которой создается Anchor
        # attr = {"queue_column":"","name":"","datatype":"","length":"","scale":"","anchor_column":""}
        # anchor_column - источник для rk в таблице attr
        # update_clmn_source - id атрибуты таблицы stage queue для update_dttm
        name=str(name).lower()
        attr_name=str(attr.get("name",None)).lower()
        db_name=name+"_"+attr_name+"_attr"
        # проверки
        # проверка на наличие такой таблицы
        exist_table=Metadata.Metadata.select_meta("anchor_table",None,0,{"name":db_name})
        if len(exist_table)>0:
            error = Support.Support.error_output("DDL","create_attribute_table","Attribute table '"+str(db_name)+"' already exists")
            sys.exit(error)
        # проверка, что указанный id queue_column существует
        for a in attr.get("queue_column",None):
            exist_queue_column=Metadata.Metadata.select_meta("queue_column",a,0)
            if len(exist_queue_column)==0:
                error = Support.Support.error_output("DDL","create_attribute_table","Queue column '"+str(a)+"' hasn't found in metadata")
                sys.exit(error)
        # проверка, что указанный id anchor_column существует
        exist_anchor_column=Metadata.Metadata.select_meta("anchor_column",attr.get("anchor_column",None),0)
        if len(exist_anchor_column)==0:
            error = Support.Support.error_output("DDL","create_attribute_table","Anchor column '"+str(attr.get("anchor_column",None))+"' hasn't found in metadata")
            sys.exit(error)
        # запись в метаданные
        # атрибуты
        table_attr=[
            {
                "name":attr.get("name",None),
                "datatype":attr.get("datatype",None),
                "length":attr.get("length",None),
                "scale":attr.get("scale",None),
                "queue_column":attr.get("queue_column",None),
                "type":"attribute",
                "deleted":0
            },
            {"name":name+"_rk","datatype":Driver.Driver.convert_to_guid(),"type":"pk","idmap_column":[attr.get("idmap_column",None)],"deleted":0},
            {"name":"update_dttm","datatype":Driver.Driver.convert_to_timestamp(),"type":"tech","deleted":0},
            {"name":"processed_dttm","datatype":Driver.Driver.convert_to_timestamp(),"type":"tech","deleted":0}
        ]
        table_attr_uuid=[]
        for i in table_attr:
            ins_attr=Metadata.Metadata.insert_meta("anchor_column",i, tran)
            i.update({"id":ins_attr.get("id",None)})
            """
            # >> Diveev - приведение типов данных атрибута от источника к приемнику 
            # Здесь хардкод!
            if i.get("type", None) == "attribute":
                i.update(
                    {"datatype": Driver.Driver.data_type_map(
                        from_datatype=i.get('datatype'),
                        from_dbms="mssql",
                        from_length=i.get('length')
                    ).get("to_datatype", None)}
                )
            # << Diveev 
            """
            table_attr_uuid.append(ins_attr.get("id",None))
        # таблица
        table={"name":db_name,"type":"attribute","anchor_column":table_attr_uuid,"deleted":0}
        ins_table=Metadata.Metadata.insert_meta("anchor_table",table,tran)
        table.update({"id":ins_table.get("id",None)})
        # таблица, хранящая максимальную дату обновления
        create_mxdttm_table_sql=DDL.create_max_dttm_table(ins_table.get("id",None), db_name, tran)
        # скрипт ddl
        create_table_sql=Driver.Driver.create_table_ddl("anch",table.get("id",None),table_attr)
        create_view_sql=Driver.Driver.create_view_sql("anch",db_name,"anch",table.get("id",None),table_attr)
        create_vers_view_sql=Driver.Driver.create_view_sql("anch",db_name+"_v","anch",db_name,table_attr,{"pk":[name+"_rk"],"dt":"update_dttm"})
        create_lv_view_sql=DDL.create_lv_view(ins_table.get("id",None), name+"_rk")
        create_sql=create_table_sql+create_view_sql+create_vers_view_sql+create_mxdttm_table_sql+create_lv_view_sql
        return {"sql":create_sql,"attribute_table":table.get("id",None),"attribute_column":table_attr}

    # генерация скрипта создания tie
    @staticmethod
    def create_tie_table(name1,name2,uuid1,uuid2,queue_uuid1, queue_uuid2, tran):
        # name1 - первая сущность в связи
        # name2 - вторая сущность в связи
        # uuid1, uuid2 - id rk в anchor, которые являются источником для полей в tie
        # queue_column - источник для атрибута update_dttm из stage queue
        name1=str(name1).lower()
        name2=str(name2).lower()
        db_name=name1+"_"+name2+"_tie"
        # проверки
        # проверка наименования
        exist_table=Metadata.Metadata.select_meta("anchor_table",None,0,{"name":db_name})
        if len(exist_table)>0:
            error = Support.Support.error_output("DDL","create_tie_table","Tie table '"+str(db_name)+"' already exists")
            sys.exit(error)
        # проверка uuid атрибутов источников
        exist_uuid1=Metadata.Metadata.select_meta("idmap_column",uuid1,0)
        if len(exist_uuid1)==0:
            error = Support.Support.error_output("DDL","create_tie_table","Idmap column '"+str(uuid1)+"' hasn't found in metadata")
            sys.exit(error)
        # проверка uuid атрибутов источников
        exist_uuid2=Metadata.Metadata.select_meta("idmap_column",uuid2,0)
        if len(exist_uuid2)==0:
            error = Support.Support.error_output("DDL","create_tie_table","Idmap column '"+str(uuid2)+"' hasn't found in metadata")
            sys.exit(error)
        # метаданные
        # атрибуты
        table_attr=[
            {"name":name1+"_rk","datatype":Driver.Driver.convert_to_guid(),"type":"pk","idmap_column":[uuid1],"queue_column":queue_uuid1,"deleted":0},
            {"name":name2+"_rk","datatype":Driver.Driver.convert_to_guid(),"type":"pk","idmap_column":[uuid2],"queue_column":queue_uuid2,"deleted":0},
            {"name":"update_dttm","datatype":Driver.Driver.convert_to_timestamp(),"type":"tech","deleted":0},
            {"name":"processed_dttm","datatype":Driver.Driver.convert_to_timestamp(),"type":"tech","deleted":0}
        ]
        attr_uuid=[]
        for i in table_attr:
            ins_attr=Metadata.Metadata.insert_meta("anchor_column",i,tran)
            i.update({"id":ins_attr.get("id",None)})
            attr_uuid.append(ins_attr.get("id",None))
        # таблица
        table={"name":db_name,"type":"tie","anchor_column":attr_uuid,"deleted":0}
        ins_table=Metadata.Metadata.insert_meta("anchor_table",table,tran)
        # скрипты
        create_table_sql=Driver.Driver.create_table_ddl("anch",ins_table.get("id",None),table_attr)
        create_view_sql=Driver.Driver.create_view_sql("anch",db_name,"anch",ins_table.get("id",None),table_attr)
        create_vers_view_sql=Driver.Driver.create_view_sql("anch",db_name+"_v","anch",db_name,table_attr,{"pk":[name1+"_rk",name2+"_rk"],"dt":"update_dttm"})
        create_sql=create_table_sql+create_view_sql+create_vers_view_sql
        return {"sql":create_sql,"tie_table":ins_table.get("id",None),"tie_column":table_attr}

    #  генерация техничесой таблицы, хранящей ключ и максимальную дату атрибута
    # только для таблиц типа atteibute!
    @staticmethod
    def create_max_dttm_table(table_id, table_name, tran):
        #  table_name - наименование таблицы
        # table_id - uuid таблицы
        # rk - наименование rk сущности
        table_nm=str(table_name).lower()+"_mxdttm"
        # проверка наименования
        exist_table=Metadata.Metadata.select_meta("table_max_dttm",None,0,{"name":table_nm})
        if len(exist_table)>0:
            error = Support.Support.error_output("DDL","create_max_dttm_table","Max dttm table '"+str(table_nm)+"' already exists")
            sys.exit(error)
        # проверка существования подаваемой таблицы
        exist_anchor_table=Metadata.Metadata.select_meta("anchor_table",table_id,0,None)
        if len(exist_anchor_table)==0:
            error = Support.Support.error_output("DDL","create_max_dttm_table","Anchor table '"+str(table_name)+"' doesn't exists")
            sys.exit(error)
        # проверка, что подаваемая таблиа типа attribute
        if exist_anchor_table[0].get("type",None)!="attribute":
            error = Support.Support.error_output("DDL","create_max_dttm_table","The type of anchor table '"+str(table_name)+"' is not 'attribute'")
            sys.exit(error)
        # таблица
        table={"name":table_nm,"anchor_table":[table_id],"deleted":0}
        ins_table=Metadata.Metadata.insert_meta("table_max_dttm",table,tran)
        table_attr=[ # заместо id подаем наименование атрибута, так как незачем генерить лишние метаданные, когда наименование атрибутов не будут меняться
            {"name":"rk","datatype":Driver.Driver.convert_to_guid(),"id":"rk"},
            {"name":"max_update_dttm","datatype":Driver.Driver.convert_to_timestamp(),"id":"max_update_dttm"}
        ]
        # скрипты
        create_table_sql=Driver.Driver.create_table_ddl("anch",ins_table.get("id",None),table_attr)
        create_view_sql=Driver.Driver.create_view_sql("anch",table_nm,"anch",ins_table.get("id",None),table_attr)
        create_sql=create_table_sql+create_view_sql
        return create_sql

    # генерация скрипта view на актуальный срез
    # работает только для таблиц типа attribute
    @staticmethod
    def create_lv_view(table_id, entity_rk):
        # table_id - uuid таблицы, для которой будет делаться view на актуальный срез
        # entity_rk - rk сущности
        # проверка существования подаваемой таблицы
        exist_anchor_table=Metadata.Metadata.select_meta("anchor_table",table_id,0,None)
        if len(exist_anchor_table)==0:
            error = Support.Support.error_output("DDL","create_lv_view","Anchor table '"+str(table_id)+"' doesn't exists")
            sys.exit(error)
        # проверка существования технической таблицы хранящей актуальную версию
        exist_max_table=Metadata.Metadata.parent_select_meta("table_max_dttm","anchor_table",table_id,0)
        if len(exist_anchor_table)==0:
            error = Support.Support.error_output("DDL","create_lv_view","Max dttm table for anchor table'"+str(table_id)+"' doesn't exists")
            sys.exit(error)
        # наименование view на актуальный срез
        view_name=str(exist_anchor_table[0].get("name",None))
        lv_view_name=view_name+"_lv"
        # формируем скрипт
        create_lv_view_sql=Driver.Driver.create_lv_view_sql("anch",view_name, lv_view_name ,exist_max_table.get("name",0), entity_rk)
        return create_lv_view_sql
























