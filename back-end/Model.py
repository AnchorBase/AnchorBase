from Metadata import *
import SQLScript
import Support
import Source
import Driver
import DDL
import ETL
import pyodbc
import sys
import datetime
import json
from SystemObjects import Constant as const
from Source import Source as Source
import uuid
from DWH import *
from collections import Counter

class Model:
    """
    Модель данных
    Принимает на вход json и создает модель
    """

    # TODO: статические методы - устаревшие

    # создание якорной модели на основе заданной бизнес-модели
    # [
    #     {
    #         "name":"",
    #         "desc":"описание сущности или факта",
    #         "attribute":[{
    #             "name":"наименование атрибута (на английском)",
    #             "desc":"описание атрибута",
    #             "pk":"признак ключа сущности",
    #             "foreign_entity":"наименование сущности, на которую ссылается атрибут",
    #             "datatype":"тип данных",
    #             "length":"длина",
    #             "scale":"символов после запятой",
    #             "source":[{
    #                 "name":"наименование источника",
    #                 "schema":"схема таблицы источника",
    #                 "table":"наименование таблицы источника",
    #                 "column":"наименование атрибута источника"
    #             }]
    #         }]
    #     }
    # ]
    # !!! если у сущности на разных источниках разные поля бизнес-ключи (id, ИНН  к примеру), их нужно указывать в качестве источинка у pk сущности
    @staticmethod
    def _create_model(model, tran):
        # проверки бизнес-модели
        exist_entity_meta=Metadata.Metadata.select_meta("entity",None,0) # уже созданные сущности
        exist_entity_list=[]
        entity_list=[] # сущности, указанные в модели
        attribute_list=[]
        source_attr=[]
        source_table=[]
        source_column=[]
        for e in exist_entity_meta:
            exist_entity_list.append(e.get("name",None))
        for a in model:
            pk_check=0
            # проверка, что у сущности указаны атрибуты
            if a.get("attribute",None) is None:
                error = Support.Support.error_output("Model","create_model","Entity '"+str(a.get("name",None)+"' has no attibute"))
                sys.exit(error)
            for b in a.get("attribute",None):
                if b.get("pk",None) is not None:
                    pk_check=pk_check+1
                # проверка, что у сущности не дублируются атрибуты
                if str(a.get("name",None))+"."+str(b.get("name",None)) in attribute_list:
                    error = Support.Support.error_output("Model","create_model","Attribute '"+str(b.get("name",None)+"' is specified more than once"))
                    sys.exit(error)
                attribute_list.append(str(a.get("name",None))+"."+str(b.get("name",None)))
                # проверка, что у атрибута указан источник
                if b.get("source") is None:
                    error = Support.Support.error_output("Model","create_model","Attribute '"+str(b.get("name",None)+"' has no source"))
                    sys.exit(error)
                # проверка, что у атрибута указан тип данных
                if b.get("datatype",None) is None:
                    error = Support.Support.error_output("Model","create_model","Attribute '"+str(b.get("name",None)+"' has no datatype"))
                    sys.exit(error)
                # отбираем атрибуты источника
                for d in b.get("source",None):
                    exist_source=Metadata.Metadata.select_meta("source",None,0,{"name":d.get("name",None)})
                    # проверка, что источник существует
                    if len(exist_source)==0:
                        error = Support.Support.error_output("Model","create_model","Source '"+str(d.get("id",None)+"' doesn't exist"))
                        sys.exit(error)
                    else:
                        d.update({"id":exist_source[0].get("id",None)})

                    source_cnct=Metadata.Metadata.select_meta("source",d.get("id",None)) # отбираем параметры подключения источника
                    # проверяем, что указанная таблица и атрибут источника существуют на источнике
                    exist_source_obj=Driver.Driver.select_db_object(
                        source_cnct[0]
                        ,{
                            "database":source_cnct[0].get("database",None),
                            "schema":d.get("schema",None),
                            "table":d.get("table",None)
                        })
                    if len(exist_source_obj)==0:
                        error = Support.Support.error_output("Model","create_model","Source table '"+str(d.get("table",None)+"' doesn't exist"))
                        sys.exit(error)
                    exist_source_attr=[]
                    source_datatype=""
                    source_length=""
                    source_scale=""
                    for e in exist_source_obj:
                        exist_source_attr.append(e[4].lower())
                        if d.get("column",None).lower()==e[4].lower():
                            source_datatype=e[5].lower()
                            source_length=e[6]
                            if e[8]==0:
                                e[8]=None
                            source_scale=e[8]
                    if d.get("column",None).lower() not in exist_source_attr:
                        error = Support.Support.error_output("Model","create_model","Source column '"+str(d.get("column",None)+"' doesn't exist"))
                        sys.exit(error)
                    # отбираем атрибуты источника
                    if str(d.get("schema",None))+"."+str(d.get("table",None)) not in source_table:
                        source_table.append(str(d.get("schema",None))+"."+str(d.get("table",None)))
                        source_column.append(str(d.get("schema",None))+"."+str(d.get("table",None))+"."+str(d.get("column",None)))
                        source_attr.append(
                            {
                                "id":d.get("id",None),
                                "schema":d.get("schema",None),
                                "table":d.get("table",None),
                                "column":[{
                                    "name":d.get("column",None),
                                    "datatype":source_datatype,
                                    "length":source_length,
                                    "scale":source_scale,
                                    "entity_column_key":str(a.get("name",None))+"."+str(b.get("name",None)) # записываем Сущность.Атрибут для удобства поиска
                                }]
                            }
                        )
                    elif str(d.get("schema",None))+"."+str(d.get("table",None))+"."+str(d.get("column",None)) not in source_column:
                        for f in source_attr:
                            if d.get("schema",None)==f.get("schema",None) and d.get("table",None)==f.get("table",None):
                                source_column.append(str(d.get("schema",None))+"."+str(d.get("table",None))+"."+str(d.get("column",None)))
                                f.get("column",None).append({
                                    "name":d.get("column",None),
                                    "datatype":source_datatype,
                                    "length":source_length,
                                    "scale":source_scale,
                                    "entity_column_key":str(a.get("name",None))+"."+str(b.get("name",None)) # записываем Сущность.Атрибут для удобства поиска
                                })
            # проверка наличия pk у сущности
            if pk_check==0:
                error = Support.Support.error_output("Model","create_model","Entity '"+str(a.get("name",None))+"' hasn't any primary key")
                sys.exit(error)
            # проверка наименования сущности
            if a.get("name",None) in exist_entity_list:
                error = Support.Support.error_output("Model","create_model","Entity '"+str(a.get("name",None))+"' already exists")
                sys.exit(error)
            # проверка, что одна и та же сущность не указана дважды
            if a.get("name",None) in entity_list:
                error = Support.Support.error_output("Model","create_model","Entity '"+str(a.get("name",None))+"' is specified more than once")
                sys.exit(error)
            entity_list.append(a.get("name",None))
        # проверка, что указанная внешняя сущность либо создается, либо уже создана
        for c in model:
            for f in c.get("attribute",None):
                if f.get("foreign_entity",None) not in entity_list and f.get("foreign_entity",None) not in exist_entity_list and f.get("foreign_entity",None) is not None:
                    error = Support.Support.error_output("Model","create_model","Entity '"+str(f.get("foreign_entity",None))+"' doesn't exist")
                    sys.exit(error)
        # создание stage
        stage_ddl=""
        for h in source_attr:
            # queue
            create_stage=DDL.DDL.create_stage_table(
                h.get("table",None),
                h.get("column",None),
                h.get("schema",None),
                tran,
                h.get("id",None)
            )
            h.update({"queue_table":create_stage.get("queue_table",None),"storage_table":create_stage.get("storage_table",None)})
            stage_ddl=stage_ddl+create_stage.get("sql",None)+ "\n"
            # генерация ETL
            ETL.ETL.create_queue_etl(create_stage.get("queue_table",None),tran)
        idmap_ddl=""
        anchor_ddl=""
        attribute_ddl=""
        tie_ddl=""
        for i in model:
            entity_attribute_uuid_list=[] # лист с uuid атрибутов сущности
            # создание idmap
            idmap_src_clmn_uuid=[]
            # вытаскиваем uuid атрибута stage, по которому будет генерироваться суррогат
            for j in i.get("attribute",None):
                if j.get("pk",None)==1:
                    for k in source_attr:
                        for l in k.get("column",None):
                            if l.get("entity_column_key",None)==str(i.get("name",None))+"."+str(j.get("name",None)):
                                idmap_src_clmn_uuid.append(l.get("queue_column",None))
            # создаем idmap
            create_idmap=DDL.DDL.create_idmap_ddl(i.get("name",None),idmap_src_clmn_uuid,tran)
            idmap_ddl=idmap_ddl+create_idmap.get("sql",None)+ "\n"
            # генерация ETL
            ETL.ETL.create_idmap_etl(create_idmap.get("idmap",None),tran)
            # создание anchor
            create_anchor=DDL.DDL.create_anchor_table(i.get("name",None),create_idmap.get("idmap_rk",None),tran)
            anchor_ddl=anchor_ddl+create_anchor.get("sql",None)+ "\n"
            # генерация ETL
            ETL.ETL.create_anchor_etl(create_anchor.get("anchor_table",None),tran)
            # создание attribute
            for m in i.get("attribute",None):
                # вытаскиваем queue_column uuid
                queue_clmn_id=[]
                for n in source_attr:
                    for o in n.get("column",None):
                        if o.get("entity_column_key",None)==str(i.get("name",None))+"."+str(m.get("name",None)):
                            queue_clmn_id.append(o.get("queue_column",None))
                # вытаскиваем uuid anchor_rk
                anchor_rk_uuid=""
                for p in create_anchor.get("anchor_column",None):
                    if p.get("type",None)=="pk":
                        anchor_rk_uuid=p.get("id",None)
                attribute_attr={
                    "queue_column":queue_clmn_id,
                    "name":m.get("name",None),
                    "datatype":m.get("datatype",None),
                    "length":m.get("length",None),
                    "scale":m.get("scale",None),
                    "idmap_column":create_idmap.get("idmap_rk",None)
                }
                create_attribute=DDL.DDL.create_attribute_table(i.get("name",None),attribute_attr,tran)
                # генерация ETL
                ETL.ETL.create_attribute_etl(create_attribute.get("attribute_table",None),tran)
                attribute_ddl=attribute_ddl+create_attribute.get("sql",None)+ "\n"
                # создание tie
                # вытаскиаем uuid idmap_column внешней сущности для tie
                link_queue_column_uuid=[]
                if m.get("foreign_entity",None) is not None:
                    idmap_column_uuid_meta=Metadata.Metadata.select_meta("idmap_column",None,0,{"type":"rk"})
                    for o in m.get("source",None):
                        queue_table_uuid_meta=Metadata.Metadata.select_meta("queue_table",None,0,None)
                        for n in queue_table_uuid_meta:
                            queue_key=[]
                            for p in n.get("queue_column",None):
                                queue_column_uuid_meta=Metadata.Metadata.select_meta("queue_column",p,0,None)[0]
                                source_table_uuid_meta=Metadata.Metadata.select_meta("source_table",n.get("source_table",None),0,None)[0]
                                source_meta=Metadata.Metadata.select_meta("source",source_table_uuid_meta.get("source",None),0,None)[0]
                                # формируем ключ для поиска
                                queue_key=str(source_meta.get("name",None))+"."+str(source_table_uuid_meta.get("schema",None))+"."+str(source_table_uuid_meta.get("name",None))+"."+str(queue_column_uuid_meta.get("name",None))
                                if str(o.get("name",None))+"."+str(o.get("schema",None))+"."+str(o.get("table",None))+"."+str(o.get("column",None))==queue_key:
                                    link_queue_column_uuid.append(queue_column_uuid_meta.get("id",None))

                    link_idmap_column_uuid=""
                    for r in idmap_column_uuid_meta:
                        if r.get("name",None)==str(m.get("foreign_entity",None)).lower()+"_rk":
                            link_idmap_column_uuid=r.get("id",None)

                    create_tie=DDL.DDL.create_tie_table(i.get("name",None),m.get("foreign_entity",None),create_idmap.get("idmap_rk",None),link_idmap_column_uuid,idmap_src_clmn_uuid, link_queue_column_uuid, tran)
                    #return 1
                    # генерация ETL
                    ETL.ETL.create_tie_etl(create_tie.get("tie_table",None),tran)
                    tie_ddl=tie_ddl+create_tie.get("sql",None)+ "\n"
                # запись атрибутов сущности в метаданные
                entity_attribute_attr={
                    "name":m.get("name",None),
                    "desc":m.get("desc",None),
                    "anchor_column":[anchor_rk_uuid],
                    "pk":m.get("pk",None),
                    "foreign_entity":m.get("foreign_entity",None),
                    "deleted":0
                }
                ins_entity_attribute=Metadata.Metadata.insert_meta("entity_attribute",entity_attribute_attr,tran)
                entity_attribute_uuid_list.append(ins_entity_attribute.get("id",None))
            # запись сущности в метаданные
            entity_attr={
                "name":i.get("name",None),
                "desc":i.get("desc",None),
                "entity_attribute":entity_attribute_uuid_list,
                "deleted":0
            }
            ins_entity=Metadata.Metadata.insert_meta("entity",entity_attr,tran)
        # создание объектов
        ddl=stage_ddl+idmap_ddl+anchor_ddl+attribute_ddl+tie_ddl
        return ddl

    # TODO: посылать в stage только те источники, которые не грузятся в stage
    # TODO: продумать то, что если таблица в stage грузится, а указанный атрибут нет. скорее всего придется делать alter через drop and create

    def __init__(self, p_json: str):
        """
        Конструктор

        :param p_json: json с моделью данных
        """
        self._json=p_json

        # проверки модели данных при инициализации
        self.__model_checker()

    @property
    def json(self) -> dict:
        """
        Json с параметрами модели
        """
        return json.loads(self._json)

    def __model_checker(self):
        """
        Проверки модели данных
        """
        self.__json_checker()
        self.__entity_checker()

    def __json_checker(self):
        """
        Проверяет json на заполненность
        """
        if not self._json:
            sys.exit("Json пуст")

    @property
    def entity_param(self) -> object:
        """
        Сущность
        """
        return self.__entity_checker()

    def __entity_checker(self):
        """
        Проверки сущности
        """
        l_entity=_EntityParam(
            p_name=self.json.get(const('C_ENTITY').constant_value),
            p_desc=self.json.get(const('C_DESC').constant_value),
            p_attribute_param=self.json.get(const('C_ATTRIBUTE').constant_value)
        )
        return l_entity

    def create_model(self):
        """
        Создает модель данных
        """
        # переменные для последующего использования
        l_source_table_name_list=[] # лист с уникальными наименованиями таблиц источников
        l_source_attribute_name_list=[] # лист с уникальными наименованиями атрибутов источников
        l_source_table_list=[] # лист объектов класса SourceTable - таблицы источники
        l_source_attribute_list=[] # лист объектов класса Attribute - атрибуты таблиц источников
        l_idmap_source_attribute_list=[] # лист атрибутов таблиц источников - первичные ключи
        l_attribute_table_list=[] # список таблиц атрибутов сущности
        l_tie_list=[] # список tie сущности
        #########
        # Блок обработки параметров модели и создание объектов
        ########
        # создаем сущность
        l_entity=self.__create_entity()
        # создаем якорную таблицу
        l_anchor=self.__create_anchor(p_entity=l_entity)
        #########
        # Блок обработки атрибутов
        ########
        for i_attribute_param in self.entity_param.attribute_param:
            l_attribute_source_table_list=[] # лист объектов класса SourceTable - таблицы источники, указанные у атрибута
            # создаем атрибуты сущности
            l_entity_attribute=self.__create_entity_attribute(p_entity=l_entity, p_attribute_param=i_attribute_param)
            #######
            # Блок обработки параметров источника
            #######
            for i_source_param in i_attribute_param.source_param:
                l_schema=i_source_param.schema
                l_table=i_source_param.table
                l_source_id=i_source_param.source_id
                l_column=i_source_param.column
                l_unique_source_table_name=str(l_source_id)+"_"\
                                           +str(l_schema)+"_"\
                                           +str(l_table) # формируем уникальное наименование таблицы источника
                l_unique_source_column_name=str(l_source_id)+"_"\
                                            +str(l_schema)+"_"\
                                            +str(l_table)+"_"\
                                            +str(l_column) # формируем уникальное наименование атрибута источника
                l_source_table=None
                l_source_attribute=None
                if l_unique_source_table_name not in l_source_table_name_list: # если таблица источник еще не была добавлена
                    # создаем таблицы источники
                    l_source_table=self.__create_source_table(
                        p_entity=l_entity,
                        p_source_param=i_source_param
                    )
                    # если таблица только добавляется - атрибут также новый
                    l_source_column=self.__create_source_attribute(
                        p_entity_attribute=l_entity_attribute,
                        p_source_table=l_source_table,
                        p_source_param=i_source_param
                    )
                    # добавление переменных в списки
                    l_source_table_name_list.append(l_unique_source_table_name)
                    l_source_attribute_name_list.append(l_unique_source_column_name)
                    l_source_table_list.append(l_source_table)
                    l_source_attribute_list.append(l_source_column)
                elif l_unique_source_column_name not in l_source_attribute_name_list: # если атрибут еще не был добавлен
                    for i_source_table in l_source_table_list:
                        # снова формируем уникальное наименование таблицы
                        l_unique_source_table_name_ent=str(i_source_table.source.id)+"_"\
                                                       +str(i_source_table.schema)+"_"\
                                                       +str(i_source_table.name)
                        if l_unique_source_table_name_ent==l_unique_source_table_name: # находим ранее добавленную таблицу
                            l_source_table=i_source_table
                    l_source_column=self.__create_source_attribute(
                        p_entity_attribute=l_entity_attribute,
                        p_source_table=l_source_table,
                        p_source_param=i_source_param
                    )
                    l_source_attribute_name_list.append(l_unique_source_column_name)
                    l_source_attribute_list.append(l_source_column)
                else: # если и таблица и атрибут уже были добавлены
                    for i_source_table in l_source_table_list:
                        # снова формируем уникальное наименование таблицы
                        l_unique_source_table_name_ent=str(i_source_table.source.id)+"_"\
                                                       +str(i_source_table.schema)+"_"\
                                                       +str(i_source_table.name)
                        if l_unique_source_table_name_ent==l_unique_source_table_name: # находим ранее добавленную таблицу
                            l_source_table=i_source_table
                    for i_source_attribute in l_source_attribute_list:
                        # снова формируем уникальное наименование атрибута
                        l_unique_source_attribute_name_ent=str(i_source_attribute.source_table.source.id)+"_"\
                                                       +str(i_source_attribute.source_table.schema)+"_"\
                                                       +str(i_source_attribute.source_table.name)+"_"\
                                                       +str(i_source_attribute.name)
                        if l_unique_source_attribute_name_ent==l_unique_source_column_name:
                            l_source_attribute=i_source_attribute
                    # добавляем атрибут таблицы источника в метаданные атрибута сущности
                    add_attribute(p_table=l_entity_attribute,p_attribute=l_source_attribute,p_add_table_flg=0)
                l_attribute_source_table_list.append(l_source_table)
                # если атрибут первичный ключ - добавляем его атрибуты источники в лист
                if i_attribute_param.pk==1:
                    l_idmap_source_attribute_list.append(l_source_attribute)
            ##########
            # Окончание блока обработки параметров источника
            #########
            # создаем таблицу атрибут для каждого атрибута сущности
            l_attribute=self.__create_attribute_table(p_entity=l_entity,p_entity_attribute=l_entity_attribute)
            l_attribute_table_list.append(l_attribute)
            # создаем tie, если на атрибуте сущности указана связанная сущность
            l_tie=None
            if i_attribute_param.link_entity_id:
                l_tie=self.__create_tie(
                    p_entity=l_entity,
                    p_entity_attribute=l_entity_attribute,
                    p_link_entity_id=i_attribute_param.link_entity_id,
                    p_source_table=l_attribute_source_table_list
                )
                l_tie_list.append(l_tie)
        #########
        # Окончание блока обработки атрибутов
        ########
        # создаем idmap
        l_idmap=self.__create_idmap(
            p_entity=l_entity,
            p_source_attribute=l_idmap_source_attribute_list
        )

        #########
        # Окончание блока обработки параметров модели и создание объектов
        ########

        ########
        # Блок создания DDL и ETL
        #######
        # формируем ddl
        l_ddl=self.__get_ddl(
            p_source_table=l_source_table_list,
            p_idmap=l_idmap,
            p_anchor=l_anchor,
            p_attribute_table=l_attribute_table_list,
            p_tie=l_tie_list
        )
        # проверяем ddl
        self.__ddl_checker(p_ddl=l_ddl)

        return l_ddl


    def __create_entity(self) -> object:
        """
        Создает объект сущность на основе параметров
        """
        l_entity=Entity(
            p_name=self.entity_param.name,
            p_desc=self.entity_param.desc
        )
        return l_entity

    def __create_entity_attribute(self, p_entity: object, p_attribute_param: object) -> object:
        """
        Создает атрибуты сущности на основе параметров

        :param p_entity: объект класса Entity - сущность
        :param p_attribute_param: объект класса _AttributeParam - параметры атрибута
        """
        link_entity=None
        if p_attribute_param.link_entity_id:
            link_entity=Entity(
                p_id=p_attribute_param.link_entity_id
            )

        l_entity_attribute=Attribute(
            p_name=p_attribute_param.name,
            p_desc=p_attribute_param.desc,
            p_datatype=p_attribute_param.datatype,
            p_length=p_attribute_param.length,
            p_scale=p_attribute_param.scale,
            p_link_entity=link_entity,
            p_type=const('C_ENTITY_COLUMN').constant_value,
            p_pk=p_attribute_param.pk
        )
        # добавляем атрибут в сущность
        add_attribute(p_table=p_entity,p_attribute=l_entity_attribute)

        return l_entity_attribute

    def __create_source_table(self, p_entity: object, p_source_param: object) -> object:
        """
        Создает таблицу источник на основе параметров

        :param p_entity: объект класса Entity - сущность
        :param p_attribute: объект класса Attribute - атрибут сущности
        :param p_source_param: объекта класса _SourceParam - параметры источника атрибута сущности
        """
        # сперва создаем объект класса, чтобы сформировать наименование
        l_source_table_param=SourceTable(
            p_name=p_source_param.table,
            p_source=Source(p_id=p_source_param.source_id),
            p_schema=p_source_param.schema
        )
        # ищем указанную таблицу в метаданных, если она ранее была добавлена
        l_source_table_meta_obj=search_object(
            p_type=const('C_QUEUE_TABLE_TYPE_NAME').constant_value,
            p_attrs={
                const('C_NAME').constant_value:l_source_table_param.queue_name
            }
        )
        l_source_table_meta=None
        # если таблица по наименованию найдена, создаем объект класса
        if l_source_table_meta_obj.__len__()>0:
            l_source_table_meta=SourceTable(
                p_id=l_source_table_meta_obj[0].uuid
            )
        l_source_table=l_source_table_meta or l_source_table_param # либо из метаданных, либо новый объект
        # добавялем таблицу источник в сущность
        add_table(p_object=p_entity, p_table=l_source_table)

        return l_source_table

    def __create_source_attribute(self,
                                  p_entity_attribute: object,
                                  p_source_table: object,
                                  p_source_param: object
    ) -> object:
        """
        Создает атрибут таблицы источника

        :param p_entity: объект класса Entity - сущность
        :param p_entity_attribute: Объект класса Attribute - атрибут сущности
        :param p_source_table: Объект класса SourceTable - таблица источник
        :param p_source_param: параметры источника
        """
        # ищем указанный атрибут в метаданных, если он ранее был добавлен
        l_source_attribute_meta_obj=search_object(
            # ищем по id таблицы источника и наименованию атрибута
            p_type=const('C_QUEUE_COLUMN').constant_value,
            p_attrs={
                const('C_QUEUE_TABLE_TYPE_NAME'):str(p_source_table.id),
                const('C_NAME').constant_value:p_source_param.column
            }
        )
        l_source_attribute=None
        # если атрибут найден в метаданных, создаем объект класса
        if l_source_attribute_meta_obj.__len__()>0:
            l_source_attribute=Attribute(
                p_id=l_source_attribute_meta_obj[0].uuid,
                p_type=const('C_QUEUE_COLUMN').constant_value
            )
        else: # иначе создаем новый атрибут
            l_source_attribute=Attribute(
                p_name=p_source_param.column,
                p_type=const('C_QUEUE_COLUMN').constant_value,
                p_datatype="VARCHAR", # все атрибуты таблицы источника, кроме технических имеют тип данных VARCHAR(4000)
                p_length=4000
            )
        # добавляем атрибут в таблицу источник
        add_attribute(p_table=p_source_table,p_attribute=l_source_attribute)
        # добавяем атрибут к метаданным атрибута сущности
        add_attribute(p_table=p_entity_attribute,p_attribute=l_source_attribute, p_add_table_flg=0)

        return l_source_attribute

    def __create_anchor(self, p_entity: object) -> object:
        """
        Создает якорь

        :param p_entity: объект класса Entity - сущность
        """
        # при создании новой сущности точно не существует ее якоря, поэтому даже не ищем в метаданных
        l_anchor=Anchor(
            p_entity=p_entity
        )
        return l_anchor

    def __create_attribute_table(self, p_entity: object, p_entity_attribute: object) -> object:
        """
        Создает таблицу атрибут

        :param p_entity: объекта класса Entity - сущность
        :param p_entity_attribute: объект класса Attribute - атрибут сущности
        """
        # при создании новой сущности точно не существует ее атрибутов, поэтому даже не ищем в метаданных
        l_attribute_table=AttributeTable(
            p_entity=p_entity,
            p_entity_attribute=p_entity_attribute
        )
        return l_attribute_table

    def __create_tie(self,
                     p_entity: object,
                     p_entity_attribute: object,
                     p_link_entity_id: str,
                     p_source_table: list
    ) -> object:
        """
        Создает tie

        :param p_entity: сущность
        :param p_entity_attribute: атрибут сущности (внешний ключ)
        :param p_link_entity_id: id связанной сущности
        :param p_source_table: список таблиц источников
        """
        # инициализируем объект связанной сущности по ее id
        l_link_entity=Entity(
            p_id=p_link_entity_id
        )
        # при создании новой сущности нет ее tie, поэтому даже не ищем в метаданных
        l_tie=Tie(
            p_entity=p_entity,
            p_entity_attribute=p_entity_attribute,
            p_link_entity=l_link_entity,
            p_source_table=p_source_table
        )
        return l_tie

    def __create_idmap(self, p_entity: object, p_source_attribute: list) -> object:
        """
        Создает idmap сущности

        :param p_entity: сущность
        :param p_source_attribute: список атрибутов, участвующих в генерации суррогата (натуральные ключи)
        """
        # при создании сущности нет ее idmap, поэтому не ищем в метаданных
        l_idmap=Idmap(
            p_entity=p_entity,
            p_source_attribute_nk=p_source_attribute
        )
        return l_idmap

    def __get_ddl(self,
                  p_source_table: list,
                  p_idmap: object,
                  p_anchor: object,
                  p_attribute_table: list,
                  p_tie: list
    ):
        """
        Генерирует общий скрипт DDL для всех объектов сущности

        :param p_source_table: таблица источник
        :param p_idmap: idmap сущности
        :param p_anchor: якорь cущности
        :param p_attribute_table: таблица атрибут сущности
        :param p_tie: связь сущности
        """
        l_ddl=[] # список со всеми DDL объектов сущности

        # формируем скрипт для таблиц источников
        for i_source_table in p_source_table: # таблиц источников у сущности может быть несколько
            # сперва удаление таблицы источника (если существует)
            l_ddl.append(
                drop_table_ddl(p_table=i_source_table)
            )
            # создание таблицы источника
            l_ddl.append(
                create_table_ddl(p_table=i_source_table)
            )
            l_ddl.append(
                create_view_ddl(p_table=i_source_table)
            )
        # формируем скрипт для idmap
        l_ddl.append(
            create_table_ddl(p_table=p_idmap)
        )
        l_ddl.append(
            create_view_ddl(p_table=p_idmap)
        )
        # формируем скрипт для anchor
        l_ddl.append(
            create_table_ddl(p_table=p_anchor)
        )
        l_ddl.append(
            create_view_ddl(p_table=p_anchor)
        )
        # формируем скрипт для attribute
        for i_attribute_table in p_attribute_table: # может быть несколько
            l_ddl.append(
                create_table_ddl(p_table=i_attribute_table)
            )
            l_ddl.append(
                create_view_ddl(p_table=i_attribute_table)
            )
        # формируем скрипт для tie
        for i_tie in p_tie:  # может быть несколько
            l_ddl.append(
                create_table_ddl(p_table=i_tie)
            )
            l_ddl.append(
                create_view_ddl(p_table=i_tie)
            )
        return l_ddl

    def __ddl_checker(self, p_ddl: list):
        """
        Проверка DDL сущности

        :param p_ddl: DDL сущности
        """
        # запускаем ddl в транзакции, в конце скрипта явная ошибка
        # если ошибка, которую выдает СУБД, отличается от задуманной ошибки - ошибка в ddl
        l_incorrect_sql="SELECT 1 FROM 1;" # запрос с явной ошибкой
        # конкретный текст ошибки
        l_error_text='syntax error at or near "1"'
        l_error_sql_example='SELECT 1 FROM 1;'
        l_ddl="" # строка с ddl
        for i_ddl in p_ddl:
            l_ddl=l_ddl+i_ddl+"\n"
        l_ddl=l_ddl+l_incorrect_sql
        l_result=Connection().sql_exec(p_sql=l_ddl) # запускаем ddl с явной ошибкой
        if l_error_text in str(l_result[1]) and l_error_sql_example in str(l_result[1]):
            pass
        else:
            sys.exit("В сформированном DDL ошибка \n"+str(l_result[1]))










class _SourceParam:
    """
    Параметры источника атрибута
    """

    def __init__(self,
                 p_source_id: str,
                 p_table: str,
                 p_column: str,
                 p_schema: str ="public"
                 ):
        """
        Конструктор

        :param p_source: id источника
        :param p_schema: схема источника
        :param p_table: таблица источника
        :param p_column: столбец таблицы источника
        """
        self._source_id=p_source_id
        self._schema=p_schema
        self._table=p_table
        self._column=p_column

        # проверка параметров источника при инициализации
        self.__source_param_checker()


    @property
    def source_id(self) -> str:
        """
        Id источника
        """
        return self._source_id

    @property
    def schema(self) -> str:
        """
        Схема источника
        """
        return self._schema.lower()

    @property
    def table(self) -> str:
        """
        Таблица источника
        """
        return self._table.lower()

    @property
    def column(self) -> str:
        """
        Столбец источника
        """
        return self._column.lower()

    def __source_param_checker(self):
        """
        Проверяет на корректность параметры источника атрибута
        """
        self.__source_id_checker()
        self.__table_checker()
        self.__column_checker()


    def __source_id_checker(self):
        """
        Проверяет id источника
        """
        if not self.source_id:
            sys.exit("Источник не заполнен") #TODO переделать
        # проверка на корректное заполнение - uuid
        try:
            uuid.UUID(self.source_id)
        except ValueError as e:
            sys.exit("Некорректное id источника")
        # проверка на существование в метаданных источника (просто инициализируем объект)
        l_source_meta=Source(p_id=self.source_id)

    def __table_checker(self):
        """
        Проверяет таблицу источника
        """
        if not self.table:
            sys.exit("Таблица источника не заполнена")

    def __column_checker(self):
        """
        Проверяет столбец источника
        :return:
        """
        if not self.column:
            sys.exit("Столбец источника не заполнен")

class _AttributeParam:
    """
    Параметры атрибута сущности
    """
    def __init__(self,
                  p_name: str,
                  p_datatype: str,
                  p_source_param: list,
                  p_length: int =None,
                  p_scale: int =None,
                  p_link_entity_id: str =None,
                  p_desc: str =None,
                  p_pk: int =0
    ):
         """
         Конструктор

         :param p_name: наименование
         :param p_datatype: тип данных
         :param p_length: длина
         :param p_scale: кол-во цифр после запятой
         :param p_link_entity_id: id связанной сущности
         :param p_desc: описание атрибута
         :param p_pk: признак первичного ключа

         """
         self._name=p_name
         self._datatype=p_datatype
         self._legnth=p_length
         self._scale=p_scale
         self._link_entity_id=p_link_entity_id
         self._desc=p_desc
         self._pk=p_pk
         self._source_param=p_source_param

        # проверка параметров во время инициализации
         self.__attribute_param_checker()

    @property
    def name(self) -> str:
        """
        Наименование атрибута
        """
        return self._name.lower()

    @property
    def datatype(self) -> str:
        """
        Тип данных атрибута
        """
        return self._datatype

    @property
    def length(self) -> int:
        """
        Длина атрибута
        """
        return self._legnth

    @property
    def scale(self) -> int:
        """
        Количесто знаков после запятой
        """
        return self._scale

    @property
    def link_entity_id(self) -> str:
        """
        Id связанной сущности
        """
        return self._link_entity_id

    @property
    def desc(self) -> str:
        """
        Описание атрибута
        """
        return self._desc

    @property
    def pk(self) -> int:
        """
        Признак первичного ключа сущности
        """
        return self._pk

    @property
    def source_param(self) -> list:
        """
        Параметры источника атрибута
        """
        return self.__source_checker()

    def __attribute_param_checker(self):
        """
        Проверяет корректность параметров атрибута
        """
        self.__name_checker()
        self.__datatype_checker()
        self.__link_entity_checker()
        self.__source_checker()
        self.__pk_checker()

    def __name_checker(self):
        """
        Проверяет правильность наименования атрибута
        """
        if not self.name:
            sys.exit("Не задано наименование атрибута")

    def __datatype_checker(self):
        """
        Проверяет правильность типа данных
        """
        if not self.datatype:
            sys.exit("Не указан тип данных у атрибута")

    def __link_entity_checker(self):
        """
        Проверяет корректность заданной связанной сущности
        """
        if self.link_entity_id:
            try:
                uuid.UUID(self.link_entity_id)
            except ValueError as e:
                sys.exit("Некорректное id связанной сущности")
            # проверка на существование сущности в метаданных источника (просто инициализируем объект)
            l_entity_meta=Entity(p_id=self.link_entity_id)

    def __source_checker(self):
        """
        Проверяет корректность источника атрибута
        """
        if not self._source_param:
            sys.exit("У атрибута не указан ни один источник")
        else:
            l_source_table_name_list=[] # список уникальных наименований таблиц источников
            l_source=[]
            for i_source in self._source_param:
                l_source_id=i_source.get(const('C_SOURCE').constant_value)
                l_source_table=i_source.get(const('C_TABLE').constant_value)
                l_source_schema=i_source.get(const('C_SCHEMA').constant_value)
                l_source_attribute=i_source.get(const('C_COLUMN').constant_value)
                # формируем уникальное наименование таблицы источника
                l_unique_source_table_name=str(l_source_id)+"_"\
                                           +str(l_source_schema)+"_"\
                                           +str(l_source_table)
                if l_unique_source_table_name in l_source_table_name_list:
                    sys.exit("Таблица источник "+str(l_source_table)+" указана два раза у атрибута "+self._name)
                l_source.append(
                    _SourceParam(
                        p_source_id=l_source_id,
                        p_table=l_source_table,
                        p_schema=l_source_schema,
                        p_column=l_source_attribute
                    )
                )
                l_source_table_name_list.append(l_unique_source_table_name)
            return l_source

    def __pk_checker(self):
        """
        Проверка на целое число
        """
        if type(self._pk).__name__!="int":
            sys.exit("Некорректное значение признака первичного ключа")
        elif self._pk>1:
            sys.exit("Некорректное значение признака первичного ключа")


class _EntityParam:
    """
    Параметры сущности
    """
    def __init__(self,
                 p_name: str,
                 p_attribute_param: list,
                 p_desc: str =None
    ):
        """
        Конструктор

        :param p_name: наименование сущности
        :param p_attribute: атрибуты сущности
        :param p_desc: описание сущности
        """

        self._name=p_name
        self._attribute_param=p_attribute_param
        self._desc=p_desc

        # проверка параметров во время инициализации
        self.__entity_checker()

    @property
    def name(self):
        """
        Наименование сущности
        """
        return self._name.lower()

    @property
    def attribute_param(self) -> list:
        """
        Атрибуты сущности
        """
        return self.__attribute_checker()

    @property
    def desc(self):
        """
        Описание сущности
        """
        return self._desc

    def __entity_checker(self):
        """
        Проверка параметров сущности
        :return:
        """
        self.__name_checker()
        self.__attribute_checker()
        self.__source_table_pk_checker()
        self.__attribute_double_checker()
        self.__entity_name_double_checker()

    def __name_checker(self):
        """
        Проверяет корректность наименования сущности
        """
        if not self._name:
            sys.exit("У сущности не указано наименование")

    def __attribute_checker(self):
        """
        Проверка параметров атрибутов
        """
        l_attribute=[]
        if not self._attribute_param:
            sys.exit("У сущности нет ни одного атрибута")
        else:
            l_pk_cnt=0
            for i_attribute in self._attribute_param:
                l_attribute.append(
                    _AttributeParam(
                        p_name=i_attribute.get(const('C_NAME').constant_value),
                        p_desc=i_attribute.get(const('C_DESC').constant_value),
                        p_pk=i_attribute.get(const('C_PK').constant_value),
                        p_datatype=i_attribute.get(const('C_DATATYPE').constant_value),
                        p_length=i_attribute.get(const('C_LENGTH').constant_value),
                        p_scale=i_attribute.get(const('C_SCALE').constant_value),
                        p_link_entity_id=i_attribute.get(const('C_LINK_ENTITY').constant_value),
                        p_source_param=i_attribute.get(const('C_SOURCE').constant_value)
                    )
                )
                l_pk_cnt=l_pk_cnt+i_attribute.get(const('C_PK').constant_value)
            if l_pk_cnt==0:
                sys.exit("Не указано ни одного первичного ключа у сущности")

        return l_attribute

    def __source_table_pk_checker(self):
        """
        Проверяет, что все таблицы источники фигурируют, как источники у pk атрибута
        """
        l_source_table=[] # список с наименованиями таблиц источников
        l_source_table_pk=[] # список с наименованиями таблиц источников, указанных у pk атрибута
        for i_attribute in self.attribute_param:
            for i_source in i_attribute.source_param:
                l_source_table.append(str(i_source.source_id)+"_"+str(i_source.schema)+"_"+str(i_source.table))
                if i_attribute.pk==1:
                    l_source_table_pk.append(str(i_source.source_id)+"_"+str(i_source.schema)+"_"+str(i_source.table))
        for i_source_table in l_source_table:
            if i_source_table not in l_source_table_pk:
                sys.exit("Таблица источник не указана в качестве источника у первичного ключа")

    def __attribute_double_checker(self):
            """
            Проверка на дублирующийся атрибут сущности
            """
            l_attribute_list=[]
            for i_attribute in self.attribute_param:
                l_attribute_list.append(i_attribute.name)
                l_attribute_cnt=Counter(l_attribute_list).get(i_attribute.name)
                if l_attribute_cnt>1:
                    sys.exit("Атрибут "+i_attribute.name+" указан больше одного раза")

    def __entity_name_double_checker(self):
        """
        Проверяет, что в метаданных нет сущности с таким наименованием
        """
        l_entity_meta_obj=search_object(
            p_type=const('C_ENTITY').constant_value,
            p_attrs={
                const('C_NAME').constant_value:self.name
            }
        )
        if l_entity_meta_obj.__len__()>0:
            sys.exit("Сущность с таким наименование уже существует")













