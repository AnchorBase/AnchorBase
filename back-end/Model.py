import Metadata
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
from DWH import Entity as Entity
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
    def create_model(model, tran):
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
    def entity(self) -> object:
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
            p_attribute=self.json.get(const('C_ATTRIBUTE').constant_value)
        )
        return l_entity

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
        return self._schema

    @property
    def table(self) -> str:
        """
        Таблица источника
        """
        return self._table

    @property
    def column(self) -> str:
        """
        Столбец источника
        """
        return self._column

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
                  p_source: list,
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
         self._source=p_source

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
    def source(self) -> list:
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
        if not self._source:
            sys.exit("У атрибута не указан ни один источник")
        else:
            l_source=[]
            for i_source in self._source:
                l_source.append(
                    _SourceParam(
                        p_source_id=i_source.get(const('C_SOURCE').constant_value),
                        p_table=i_source.get(const('C_TABLE').constant_value),
                        p_schema=i_source.get(const('C_SCHEMA').constant_value),
                        p_column=i_source.get(const('C_COLUMN').constant_value)
                    )
                )
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
                 p_attribute: list,
                 p_desc: str =None
    ):
        """
        Конструктор

        :param p_name: наименование сущности
        :param p_attribute: атрибуты сущности
        :param p_desc: описание сущности
        """

        self._name=p_name
        self._attribute=p_attribute
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
    def attribute(self) -> list:
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
        if not self._attribute:
            sys.exit("У сущности нет ни одного атрибута")
        else:
            l_pk_cnt=0
            for i_attribute in self._attribute:
                l_attribute.append(
                    _AttributeParam(
                        p_name=i_attribute.get(const('C_NAME').constant_value),
                        p_desc=i_attribute.get(const('C_DESC').constant_value),
                        p_pk=i_attribute.get(const('C_PK').constant_value),
                        p_datatype=i_attribute.get(const('C_DATATYPE').constant_value),
                        p_length=i_attribute.get(const('C_LENGTH').constant_value),
                        p_scale=i_attribute.get(const('C_SCALE').constant_value),
                        p_link_entity_id=i_attribute.get(const('C_LINK_ENTITY').constant_value),
                        p_source=i_attribute.get(const('C_SOURCE').constant_value)
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
        for i_attribute in self.attribute:
            for i_source in i_attribute.source:
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
            for i_attribute in self.attribute:
                l_attribute_list.append(i_attribute.name)
                l_attribute_cnt=Counter(l_attribute_list).get(i_attribute.name)
                if l_attribute_cnt>1:
                    sys.exit("Атрибут "+i_attribute.name+" указан больше одного раза")

    def __entity_name_double_checker(self):
        """
        Проверяет, что в метаданных нет сущности с таким наименованием
        """
        l_entity_meta_obj=Metadata.search_object(
            p_type=const('C_ENTITY').constant_value,
            p_attrs={
                const('C_NAME').constant_value:self.name
            }
        )
        if l_entity_meta_obj.__len__()>0:
            sys.exit("Сущность с таким наименование уже существует")













