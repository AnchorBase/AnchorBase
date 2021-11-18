
import Metadata
import Support
import Source
import Driver
import sys
import SQLScript
import pyodbc
import datetime
import json

class ETL:

    # записывает лог etl
    @staticmethod
    def etl_log(table_type, table, tran):
        nw=datetime.datetime.now().strftime("%d.%m.%y %H:%M:%S")
        log={
            table_type:table,
            "timestamp":nw,
            "etl_id":tran

        }
        Metadata.Metadata.insert_meta(table_type,log, tran)
    # перезапись последнего лога
    @staticmethod
    def last_etl_log(table_type, table, tran):
        # достаем текущий последний лог
        last_log=Metadata.Metadata.select_meta(table_type, None,0,{table_type:table})
        nw=datetime.datetime.now().strftime("%d.%m.%y %H:%M:%S")
        log={
            table_type:table,
            "timestamp":nw,
            "etl_id":tran
        }
        if len(last_log)==0: # если нет последнего лога, вставляем
            ETL.etl_log(table_type, table, tran)
        else: # иначе заменяем
            Metadata.Metadata.update_meta(table_type,last_log[0].get("id",None),log, tran)


    @staticmethod
    def source_increment(source_table_id, increment, tran):
        # increment - наименование атрибута инкремента источника
        # проверка, что указанная таблица источника грузится в stage
        src_table_exist=Metadata.Metadata.select_meta("source_table",source_table_id,0)
        if len(src_table_exist)==0:
            error = Support.Support.error_output("ETL","source_increment","Source table '"+str(source_table_id)+"' doesn't exist")
            sys.exit(error)
        # проверка, что указанный атрибут таблицы источника существует
        src_id=src_table_exist[0].get("source",None)
        src_prm=Metadata.Metadata.select_meta("source",src_id,0)[0]
        src_dtbs=src_prm.get("database",None)
        src_schm=src_table_exist[0].get("schema",None)
        src_tbl_nm=src_table_exist[0].get("name",None)
        src_tbl_prm={
            "database":src_dtbs,
            "schema":src_schm,
            "table":src_tbl_nm
        }
        src_tbl_attr=Source.Source.select_source_object(src_id,src_tbl_prm)
        incr_check=0
        incr_datatype=""
        for a in src_tbl_attr[0]["schemas"][0]["tables"][0]["attributes"]:
            if a.get("attribute",None).lower()==increment.lower():
                incr_check=incr_check+1
                # проверка, что у указанного атрибута тип данных дата  или целое число
                Driver.Driver.increment_datatype_check(src_prm, a.get("datatype"))
                incr_datatype=a.get("datatype")


        if incr_check==0:
            error = Support.Support.error_output("ETL","source_increment","Attribute '"+str(increment)+"' doesn't exist")
            sys.exit(error)
        # запись в метаданные
        Metadata.Metadata.update_meta("source_table",source_table_id,{"increment":increment,"increment_datatype":incr_datatype},tran)

    # создает скрипт выбора данных с источника и загрузки их в таблицу очереди
    @staticmethod
    def create_queue_etl(queue_table, tran):
        # queue_table - uuid таблицы stage
        # проверка, что указанная таблица stage существует
        queue_table_exist=Metadata.Metadata.select_meta("queue_table",queue_table,0)
        if len(queue_table_exist)==0:
            error = Support.Support.error_output("ETL","create_queue_etl","Queue table '"+str(queue_table)+"' doesn't exist")
            sys.exit(error)
        # проверка, что у таблицы еще не существуют скрипты etl
        etl_select_exist=Metadata.Metadata.select_meta("queue_table_etl",None,0,{"type":"select source data","queue_table":queue_table})
        etl_bus_insert_exist=Metadata.Metadata.select_meta("queue_table_etl",None,0,{"type":"bus insert","queue_table":queue_table})
        etl_queue_insert_exist=Metadata.Metadata.select_meta("queue_table_etl",None,0,{"type":"queue insert","queue_table":queue_table})
        if len(etl_select_exist)>0:
            error = Support.Support.error_output("ETL","create_queue_etl","ETL script 'select source data' of queue table '"+str(queue_table)+"' already exists")
            sys.exit(error)
        if len(etl_bus_insert_exist)>0:
            error = Support.Support.error_output("ETL","create_queue_etl","ETL script 'bus insert' of queue table '"+str(queue_table)+"' already exists")
            sys.exit(error)
        if len(etl_queue_insert_exist)>0:
            error = Support.Support.error_output("ETL","create_queue_etl","ETL script 'queue insert' of queue table '"+str(queue_table)+"' already exists")
            sys.exit(error)
        # вытаскиваем атрибуты таблицы
        queue_column=[]
        src_attr_uuid=""
        queue_column_uuid=queue_table_exist[0].get("queue_column",None)
        for a in queue_column_uuid:
            queue_clmn_attr=Metadata.Metadata.select_meta("queue_column",a,0)
            queue_column.append(queue_clmn_attr[0])
            if queue_clmn_attr[0].get("source_column",None) is not None:
                src_attr_uuid=queue_clmn_attr[0].get("source_column",None)
        # формируем скрипт отбора данных
        # вытаскиваем схему, таблицу и инкремент
        src_tbl_attr=Metadata.Metadata.parent_select_meta("source_table","source_column",src_attr_uuid[0],0)
        src_attr=Metadata.Metadata.select_meta("source",src_tbl_attr.get("source",None),0)[0]
        src_type=src_attr.get("type",None)
        schema=src_tbl_attr.get("schema",None)
        table=src_tbl_attr.get("name",None)
        increment=src_tbl_attr.get("increment",None)
        increment_datatype=src_tbl_attr.get("increment_datatype",None)
        src_column_nm=[]
        src_column=[]
        for b in queue_column:
            if b.get("type",None)!="tech":
                src_column_nm.append(b.get("name",None))
                src_column.append(b)
        # вставляем в атрибуты инкремент, если указан
        if increment is not None:
            src_column_nm.append(increment)
            src_column.append({"name":increment, "datatype":increment_datatype})
        select_sql=Driver.Driver.select_data_sql(schema, table, src_column_nm,increment,increment_datatype,src_attr)
        # вставляем скрипт в метаданные
        etl_select_attr={
            "type":"select source data",
            "desc":"select data from source",
            "sql":select_sql,
            "queue_table":queue_table
        }
        # скрипт создания таблицы шины и вставки в нее данных
        Metadata.Metadata.insert_meta("queue_table_etl",etl_select_attr,tran)
        if increment is not None:
            increment_datatype=Driver.Driver.data_type_map(increment_datatype,src_type,None).get("to_datatype",None) # маппим тип данных инкремента в соответсвии с СУБД ХД
            del src_column[-1] # удаляем атрибут инкремента, чтобы потом добавить с новым типом данных
            src_column.append({"name":increment,"datatype":increment_datatype})
        bus_table_nm=str(src_attr.get("name",None))+"_"+schema+"_"+table+"_bus"
        insert_sql=Driver.Driver.insert_data_into_bus(bus_table_nm,src_column)
        # вставляем скрипт в метаданные
        etl_insert_attr={
            "type":"bus insert",
            "desc":"create bus table and insert data from source",
            "sql":insert_sql,
            "queue_table":queue_table
        }
        Metadata.Metadata.insert_meta("queue_table_etl",etl_insert_attr,tran)
        # скрипт вставки данных в таблицу очереди
        # del src_column[-1] # удаляем атрибут инкремента, так как он не добавляется в таблицу очереди #TODO: переделать
        queue_table_nm=str(src_attr.get("name",None))+"_"+schema+"_"+table+"_queue"
        insert_queue_sql=Driver.Driver.insert_data_into_queue(queue_table_nm,src_column,bus_table_nm)
        # если указан инкремент, заменяем параметр на столбец
        if increment is not None:
            insert_queue_sql=insert_queue_sql.replace('@update_dttm@','"'+str(increment)+'"')
        else:
            insert_queue_sql=insert_queue_sql.replace('@update_dttm@',Driver.Driver.current_timestamp_sql())
        # вставляем скрипт в метаданные
        etl_insert_queue_attr={
            "type":"queue insert",
            "desc":"insert data into queue table from bus table",
            "sql":insert_queue_sql,
            "queue_table":queue_table
        }
        Metadata.Metadata.insert_meta("queue_table_etl",etl_insert_queue_attr,tran)

    # скрипт генерации суррогатов в idmap
    @staticmethod
    def create_idmap_etl(idmap_table, tran):
        # idmap_table - uuid idmap таблицы
        # tran - {"id":""} - транзакция
        # проверка, что указанная таблица idmap существует
        idmap_table_exist=Metadata.Metadata.select_meta("idmap",idmap_table,0)
        if len(idmap_table_exist)==0:
            error = Support.Support.error_output("ETL","create_idmap_etl","Idmap table '"+str(idmap_table)+"' doesn't exist")
            sys.exit(error)
        idmap_name=idmap_table_exist[0].get("name",None)
        # проверка, что у таблицы еще не существуют скрипты etl
        etl_select_exist=Metadata.Metadata.select_meta("idmap_etl",None,0,{"type":"generate rk","idmap_table":idmap_table})
        if len(etl_select_exist)>0:
            error = Support.Support.error_output("ETL","create_idmap_etl","ETL script 'generate rk' of idmap table '"+str(idmap_table)+"' already exists")
            sys.exit(error)
        # вытаскиваем наименования натуральных ключей и наименование таблицы queue
        nkey_list=[] # список натуральных ключей, на основе которых генерирурется суррогат, с привязкой к таблице queue
        idmap_column_uuid=idmap_table_exist[0].get("idmap_column",None)
        for a in idmap_column_uuid:
            idmap_column_attr=Metadata.Metadata.select_meta("idmap_column",a,0)[0]
            if idmap_column_attr.get("type",None)=="nk":
                for b in idmap_column_attr.get("queue_column",None):
                    queue_column_attr=Metadata.Metadata.select_meta("queue_column",b,0)[0]
                    queue_table=Metadata.Metadata.parent_select_meta("queue_table","queue_column",queue_column_attr.get("id",None),0)
                    # достаем uuid источника
                    source_id=Metadata.Metadata.select_meta("source_table",queue_table.get("source_table",None),0)[0].get("source",None)
                    # разворачиваем nkey_list для поиска в нем таблицы queue (если она добавлена)
                    nkey_list_vls=""
                    #nkey_list_vls=[] -- БАГ!!!
                    for d in nkey_list:
                        # nkey_list_vls.append(list(d.values())) -- БАГ!!!
                        nkey_list_vls = list(d.values())
                    if queue_table.get("name",None) not in nkey_list_vls:
                        nkey_list.append({"queue_table":queue_table.get("name",None),"queue_column":[],"source":source_id})
                    for c in nkey_list:
                        if queue_table.get("name",None)==c.get("queue_table",None):
                            c.get("queue_column",None).append(queue_column_attr.get("name",None))
        # генерация ETL и вставка в метаданные
        # проходимся циклом, так как источников у idmap может быть несколько
        for e in nkey_list:
            idmap_etl=Driver.Driver.insert_data_into_idmap(idmap_name,e.get("queue_column",None),e.get("queue_table",None),e.get("source",None))

            etl_attr={
                "type":"generate rk",
                "desc":"determine natural keys without rk and generate rk",
                "sql":idmap_etl,
                "idmap":idmap_table
            }
            Metadata.Metadata.insert_meta("idmap_etl",etl_attr,tran)

    # скрипт загрузки данных в anchor
    @staticmethod
    def create_anchor_etl(anchor_table,tran):
        # anchor_table - uuid таблицы anchor
        # проверка, что указанная таблица anchor существует
        anchor_table_exist=Metadata.Metadata.select_meta("anchor_table",anchor_table,0)
        if len(anchor_table_exist)==0:
            error = Support.Support.error_output("ETL","create_anchor_etl","Anchor table '"+str(anchor_table)+"' doesn't exist")
            sys.exit(error)
        anchor_name=anchor_table_exist[0].get("name",None)
        idmap_name="idmap_"+anchor_name.replace("_anch","")
        # проверка, что у таблицы еще не существуют скрипты etl
        etl_select_exist=Metadata.Metadata.select_meta("anchor_table_etl",None,0,{"type":"anchor insert","anchor_table":anchor_table})
        if len(etl_select_exist)>0:
            error = Support.Support.error_output("ETL","create_anchor_etl","ETL script 'anchor insert' of anchor table '"+str(anchor_table)+"' already exists")
            sys.exit(error)
        # генерация etl
        anchor_etl=Driver.Driver.insert_data_into_anchor(anchor_name,idmap_name)
        # вставляем скрипт в метаданные
        etl_insert_anchor_attr={
            "type":"anchor insert",
            "desc":"insert data into anchor table from idmap table",
            "sql":anchor_etl,
            "anchor_table":anchor_table
        }
        Metadata.Metadata.insert_meta("anchor_table_etl",etl_insert_anchor_attr,tran)

    # скрипт загрузки данных в attribute
    @staticmethod
    def create_attribute_etl(attribute_table, tran):
        # attribute_table - uuid таблицы attribute
        # проверка, что указанная таблица attribute существует
        attribute_table_id=str(attribute_table)
        attribute_table_exist=Metadata.Metadata.select_meta("anchor_table",attribute_table,0)
        if len(attribute_table_exist)==0:
            error = Support.Support.error_output("ETL","create_attribute_etl","Attribute table '"+str(attribute_table)+"' doesn't exist")
            sys.exit(error)
        # проверка, что у таблицы еще не существуют скрипты etl
        etl_select_exist=Metadata.Metadata.select_meta("anchor_table_etl",None,0,{"type":"attribute insert","anchor_table":attribute_table})
        if len(etl_select_exist)>0:
            error = Support.Support.error_output("ETL","create_attribute_etl","ETL script 'attribute insert' of attribute table '"+str(attribute_table)+"' already exists")
            sys.exit(error)
        # находим наименование таблицы attribute и формируем наименования для ее _lv и _mxdttm
        attribute_table=attribute_table_exist[0].get("name",None)
        attribute_lv_view=str(attribute_table)+"_lv"
        attribute_mxdttm_table=str(attribute_table)+"_mxdttm"
        # вытаскиваем наименование поля таблицы attribute, rk и наименование idmap
        attr_column_list=attribute_table_exist[0].get("anchor_column",None)
        attribute_column=""
        anchor_column=""
        idmap_table=""
        queue_obj=[]
        queue_column=[]
        queue_table=[]
        queue_key=[]
        for a in attr_column_list:
            attribute_column_meta=Metadata.Metadata.select_meta("anchor_column",a,0,None)[0]
            if attribute_column_meta.get("type",None)=="attribute":
                attribute_column=attribute_column_meta.get("name",None)
                # находим source_id, queue_table, queue_column, queue_key (список полей, участвующих в генерации суррогатов
                for b in attribute_column_meta.get("queue_column",None):
                    # находим queue_column
                    queue_attr={}
                    queue_column_meta=Metadata.Metadata.select_meta("queue_column",b,0,None)[0]
                    queue_attr.update({"queue_column":queue_column_meta.get("name",None)})
                    # находим queue_table
                    queue_table_meta=Metadata.Metadata.parent_select_meta("queue_table","queue_column",queue_column_meta.get("id",None),0)
                    queue_attr.update({"queue_table":queue_table_meta.get("name",None)})
                    # находим source_id
                    source_table_meta=Metadata.Metadata.select_meta("source_table",queue_table_meta.get("source_table",None),0,None)[0]
                    queue_attr.update({"source_id":source_table_meta.get("source",None)})
                    queue_obj.append(queue_attr)
                    # находим наименования полей queue table, которые участвуют в генерации суррогатов
            if attribute_column_meta.get("type",None)=="pk":
                anchor_column=attribute_column_meta.get("name",None)
                idmap_table_meta=Metadata.Metadata.parent_select_meta("idmap","idmap_column",attribute_column_meta.get("idmap_column",None)[0],0)
                idmap_table=idmap_table_meta.get("name",None)
                # находим наименования полей queue table, которые участвуют в генерации суррогатов
                for c in idmap_table_meta.get("idmap_column",None):
                    idmap_nk_meta=Metadata.Metadata.select_meta("idmap_column",c,0,None)[0]
                    if idmap_nk_meta.get("type",None)=="nk":
                        for d in idmap_nk_meta.get("queue_column",None):
                            queue_key_meta=Metadata.Metadata.select_meta("queue_column",d,0,None)[0]
                            queue_key_table_meta=Metadata.Metadata.parent_select_meta("queue_table","queue_column",queue_key_meta.get("id",None),0)
                            queue_key.append(
                                {"queue_table":queue_key_table_meta.get("name",None),
                                 "queue_column":queue_key_meta.get("name",None)
                            })
        # подтягиваем к queue_obj queue_key по наименованию таблицы stage
        for d in queue_obj:
            queue_key_list=[]
            for e in queue_key:
                if d.get("queue_table",None)==e.get("queue_table",None):
                    queue_key_list.append(e.get("queue_column",None))
            d.update({"queue_key":queue_key_list})
        idmap_key=vers_table=str(str(anchor_column)+"@@")[::-1].replace("@@kr_","kn_")[::-1] # формируем наименование nk в idmap
        attribute_etl=Driver.Driver.insert_data_into_attribute(attribute_table,attribute_lv_view,attribute_column,anchor_column,idmap_table,idmap_key, attribute_mxdttm_table, queue_obj)
        # вставляем скрипт в метаданные
        etl_insert_attribute_attr={
            "type":"attribute insert",
            "desc":"insert data into attribute table",
            "sql":attribute_etl,
            "anchor_table":attribute_table_id
        }
        Metadata.Metadata.insert_meta("anchor_table_etl",etl_insert_attribute_attr,tran)

    @staticmethod
    def create_tie_etl(tie_table,tran):
        """
        Записывает в метаданные SQL-скрипт загрузки данных в таблицу типа tie
        input:
            tie_table - наименование таблицы типа tie
        output:
            None
        """
        # проверка, что указанная таблица tie существует
        tie_table_id=str(tie_table)
        tie_table_exist=Metadata.Metadata.select_meta("anchor_table",tie_table,0)
        if len(tie_table_exist)==0:
            error = Support.Support.error_output("ETL","create_tie_etl","Tie table '"+str(tie_table)+"' doesn't exist")
            sys.exit(error)
        # проверка, что у таблицы еще не существуют скрипты etl
        etl_select_exist=Metadata.Metadata.select_meta("anchor_table_etl",None,0,{"type":"tie insert","anchor_table":tie_table})
        if len(etl_select_exist)>0:
            error = Support.Support.error_output("ETL","create_tie_etl","ETL script 'tie insert' of tie table '"+str(tie_table)+"' already exists")
            sys.exit(error)
        # вытаскиваем наименование таблицы и ее атрибутов
        tie_table=tie_table_exist[0].get("name",None)
        #TODO: Сейчас реализован костыль. В целевой картине tie должен быть привязана к idmap_column
        tie_rk1=""
        tie_rk2=""
        queue_obj=[]
        queue_table_list=[]
        idmap1_table=""
        idmap2_table=""
        idmap1_key=""
        idmap2_key=""
        for a in tie_table_exist[0].get("anchor_column",None):
            # находим anchor_column
            tie_column_meta=Metadata.Metadata.select_meta("anchor_column",a,0,None)[0]
            if tie_column_meta.get("type",None)=="pk" and tie_rk1=="":
                tie_rk1=tie_column_meta.get("name",None)
                idmap1_column_meta=Metadata.Metadata.select_meta("idmap_column",tie_column_meta.get("idmap_column",None)[0],0,None)[0]
                idmap1_key=idmap1_column_meta.get("name",None)
                idmap1_table_meta=Metadata.Metadata.parent_select_meta("idmap","idmap_column",idmap1_column_meta.get("id",None))
                idmap1_table=idmap1_table_meta.get("name",None)
                for b in tie_column_meta.get("queue_column",None):
                    queue_column1_meta=Metadata.Metadata.select_meta("queue_column",b,0,None)[0]
                    queue_column1=queue_column1_meta.get("name",None)
                    queue_table1_meta=Metadata.Metadata.parent_select_meta("queue_table","queue_column",b,0)
                    queue_table1=queue_table1_meta.get("name",None)
                    source_table_meta=Metadata.Metadata.select_meta("source_table",queue_table1_meta.get("source_table",None),0,None)[0]
                    source_meta=Metadata.Metadata.select_meta("source",source_table_meta.get("source",None),0,None)[0]
                    source1=source_meta.get("id",None)
                    if queue_table1 not in queue_table_list:
                        queue_obj.append({"source_id":source1,"queue_table":queue_table1,"queue_key1":[queue_column1]})
                        queue_table_list.append(queue_table1)
                    else:
                        for c in queue_obj:
                            if c.get("queue_table",None)==queue_table1:
                                c.get("queue_key1",None).append(queue_column1)
            # TODO: Костыль - нужно реализовать более изящно
            elif tie_column_meta.get("type",None)=="pk" and tie_rk2=="":
                tie_rk2=tie_column_meta.get("name",None)
                idmap2_column_meta=Metadata.Metadata.select_meta("idmap_column",tie_column_meta.get("idmap_column",None)[0],0,None)[0]
                idmap2_key=idmap2_column_meta.get("name",None)
                idmap2_table_meta=Metadata.Metadata.parent_select_meta("idmap","idmap_column",idmap2_column_meta.get("id",None))
                idmap2_table=idmap2_table_meta.get("name",None)
                for b in tie_column_meta.get("queue_column",None):
                    queue_column2_meta=Metadata.Metadata.select_meta("queue_column",b,0,None)[0]
                    queue_column2=queue_column2_meta.get("name",None)
                    queue_table2_meta=Metadata.Metadata.parent_select_meta("queue_table","queue_column",b,0)
                    queue_table2=queue_table2_meta.get("name",None)
                    source_table_meta=Metadata.Metadata.select_meta("source_table",queue_table2_meta.get("source_table",None),0,None)[0]
                    source_meta=Metadata.Metadata.select_meta("source",source_table_meta.get("source",None),0,None)[0]
                    source2=source_meta.get("id",None)
                    for c in queue_obj:
                            if c.get("queue_table",None)==queue_table2 and c.get("queue_key2",None) is None:
                                c.update({"queue_key2":[queue_column2]})
                            elif c.get("queue_table",None)==queue_table2 and queue_column2 not in c.get("queue_key2",None):
                                c.get("queue_key2",None).append(queue_column2)
        # генерация ETL
        idmap1_key=idmap1_key[:-3]+"_nk"
        idmap2_key=idmap2_key[:-3]+"_nk"
        tie_etl=Driver.Driver.insert_data_into_tie(tie_table,tie_rk1,tie_rk2,idmap1_table,idmap2_table,idmap1_key,idmap2_key,queue_obj)
        # вставляем скрипт в метаданные
        etl_insert_tie_attr={
            "type":"tie insert",
            "desc":"insert data into tie table",
            "sql":tie_etl,
            "anchor_table":tie_table_id
        }
        Metadata.Metadata.insert_meta("anchor_table_etl",etl_insert_tie_attr,tran)
















