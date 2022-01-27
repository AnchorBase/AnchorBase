from Metadata import *
import Source
import json
from DWH import *
from collections import Counter
from Constants import *

class Model:
    """
    Модель данных
    Принимает на вход json и создает модель
    """

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
        l_entity=_EntityParam(
            p_name=self.json.get(C_ENTITY),
            p_desc=self.json.get(C_DESC),
            p_attribute_param=self.json.get(C_ATTRIBUTE),
            p_id=self.json.get(C_ID)
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
        l_tie_params_dict={} # словарь с параметрами для создания tie
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
                    l_source_attribute=self.__create_source_attribute(
                        p_entity_attribute=l_entity_attribute,
                        p_source_table=l_source_table,
                        p_source_param=i_source_param
                    )
                    # добавление переменных в списки
                    l_source_table_name_list.append(l_unique_source_table_name)
                    l_source_attribute_name_list.append(l_unique_source_column_name)
                    l_source_table_list.append(l_source_table)
                    l_source_attribute_list.append(l_source_attribute)
                elif l_unique_source_column_name not in l_source_attribute_name_list: # если атрибут еще не был добавлен
                    for i_source_table in l_source_table_list:
                        # снова формируем уникальное наименование таблицы
                        l_unique_source_table_name_ent=str(i_source_table.source.id)+"_"\
                                                       +str(i_source_table.schema)+"_"\
                                                       +str(i_source_table.name)
                        if l_unique_source_table_name_ent==l_unique_source_table_name: # находим ранее добавленную таблицу
                            l_source_table=i_source_table
                    l_source_attribute=self.__create_source_attribute(
                        p_entity_attribute=l_entity_attribute,
                        p_source_table=l_source_table,
                        p_source_param=i_source_param
                    )
                    l_source_attribute_name_list.append(l_unique_source_column_name)
                    l_source_attribute_list.append(l_source_attribute)
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
            l_tie_entity_attribute_list=[]
            l_tie_source_table_list=[]
            if i_attribute_param.link_entity_id:
                l_tie_entity_list=l_tie_params_dict.get(i_attribute_param.link_entity_id)
                if l_tie_entity_list:
                    # получаем список уже добавленных атрибутов в качетсве ссылок на связанную сущность
                    l_tie_entity_attribute_list=l_tie_entity_list.get("entity_attribute")
                    # получаем список уже добавленных таблиц источников
                    l_tie_source_table_list=l_tie_entity_list.get("source_table")
                l_tie_entity_attribute_list.append(l_entity_attribute)
                l_tie_source_table_list.extend(l_attribute_source_table_list)
                l_tie_params_dict.update(
                    {
                        i_attribute_param.link_entity_id: # в качестве ключа id связанной сущности
                            {
                                "entity_attribute":l_tie_entity_attribute_list,
                                "source_table":l_tie_source_table_list
                            }
                    }
                )
        #########
        # Окончание блока обработки атрибутов
        ########
        # создаем idmap
        l_idmap=self.__create_idmap(
            p_entity=l_entity,
            p_source_attribute=l_idmap_source_attribute_list
        )
        # создаем tie на каждую связанную сущность
        l_link_entity_list=list(l_tie_params_dict.keys()) # получаем id всех связанных сущностей
        for i_link_entity in l_link_entity_list:
            l_source_table_unique=list(set(l_tie_params_dict.get(i_link_entity).get("source_table"))) # избавляемся от возможных дублей таблиц источников
            # создаем tie
            l_tie=self.__create_tie(
                p_entity=l_entity,
                p_link_entity_id=i_link_entity,
                p_entity_attribute=l_tie_params_dict.get(i_link_entity).get("entity_attribute"),
                p_source_table=l_source_table_unique
            )
            l_tie_list.append(l_tie)

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
            p_tie=l_tie_list,
            p_entity=l_entity
        )
        # проверяем ddl
        self.__ddl_checker(p_ddl=l_ddl)
        # проверяем метаданные
        self.__metadata_checker(
            p_entity=l_entity,
            p_source_table=l_source_table_list,
            p_idmap=l_idmap,
            p_anchor=l_anchor,
            p_attribute_table=l_attribute_table_list,
            p_tie=l_tie_list
        )
        # вставляем метаданные
        self.__create_metadata(
            p_entity=l_entity,
            p_source_table=l_source_table_list,
            p_idmap=l_idmap,
            p_anchor=l_anchor,
            p_attribute_table=l_attribute_table_list,
            p_tie=l_tie_list
        )
        # создаем таблицы и представления в ХД
        self.__create_ddl(p_ddl=l_ddl)

    def rename_entity(self):
        """
        Переименовывает сущность
        """
        # проверяем имя сущности
        self.entity_param.name_checker()
        # инициализируем объект сущности
        l_entity=Entity(
            p_id=self.entity_param.id
        ) # если id указан некорректно - проверка происходит при инициализации
        # формируем скрипт удаления всех представлений сущности
        # удаление idmap
        l_drop_sql=drop_view_ddl(p_table=l_entity.idmap)+"\n"
        # удаление anchor
        l_drop_sql+=drop_view_ddl(p_table=l_entity.anchor)+"\n"
        # удаление attribute
        for i_attribute in l_entity.attribute_table:
            l_drop_sql+=drop_view_ddl(p_table=i_attribute)+"\n"
        # удаление tie
        if l_entity.tie:
            for i_tie in l_entity.tie:
                l_drop_sql+=drop_view_ddl(p_table=i_tie)+"\n"
        # удаляем tie, где переименованная сущность - связанная
        l_link_tie_meta=meta.search_object(
            p_type=C_TIE,
            p_attrs={C_LINK_ENTITY:self.entity_param.id}
        )
        l_link_ties=None
        if l_link_tie_meta.__len__()>0:
            l_link_ties=[]
            for i_tie in l_link_tie_meta:
                l_link_tie=Tie(p_id=str(i_tie.uuid))
                l_drop_sql+=drop_view_ddl(p_table=l_link_tie)+"\n"
                l_link_ties.append(l_link_tie)
        # изменяем имя у сущности, а также у таблиц и ее атрибутов
        l_entity.name=self.entity_param.name # автоматически заменяются имена у всех производных объектов
        # отдельно заменяем наименование у tie, где переименованная сущность - связанная
        for i_link_tie in l_link_ties:
            i_link_tie.link_entity=l_entity
        # формируем скрипт создания представлений с новым наименованием
        # idmap
        l_create_sql=create_view_ddl(p_table=l_entity.idmap)+"\n"
        # anchor
        l_create_sql+=create_view_ddl(p_table=l_entity.anchor)+"\n"
        # attribute
        for i_attribute in l_entity.attribute_table:
            l_create_sql+=create_view_ddl(p_table=i_attribute)+"\n"
        # tie
        if l_entity.tie:
            for i_tie in l_entity.tie:
                l_create_sql+=create_view_ddl(p_table=i_tie)+"\n"
        # link_tie
        if l_link_ties:
            for i_link_tie in l_link_ties:
                l_create_sql+=create_view_ddl(p_table=i_link_tie)+"\n"
        l_sql=l_drop_sql+l_create_sql
        





    def __create_entity(self) -> object:
        """
        Создает объект сущность на основе параметров
        """
        # проверяем сущность
        self.entity_param.entity_creation_checker()

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
            p_type=C_ENTITY_COLUMN,
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
            p_type=C_QUEUE,
            p_attrs={
                C_NAME:l_source_table_param.queue_name
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
        # добавляем id источника в сущность
        add_source_to_object(p_object=p_entity, p_source=l_source_table.source)

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
            p_type=C_QUEUE_COLUMN,
            p_attrs={
                C_QUEUE:str(p_source_table.id),
                C_NAME:p_source_param.column
            }
        )
        l_source_attribute=None
        # если атрибут найден в метаданных, создаем объект класса
        if l_source_attribute_meta_obj.__len__()>0:
            l_source_attribute=Attribute(
                p_id=l_source_attribute_meta_obj[0].uuid,
                p_type=C_QUEUE_COLUMN,
                p_attribute_type=C_QUEUE_ATTR
            )
        else: # иначе создаем новый атрибут
            l_source_attribute=Attribute(
                p_name=p_source_param.column,
                p_type=C_QUEUE_COLUMN,
                p_attribute_type=C_QUEUE_ATTR,
                p_datatype=C_VARCHAR, # все атрибуты таблицы источника, кроме технических имеют тип данных VARCHAR(4000)
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
                     p_entity_attribute: list,
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
                  p_tie: list,
                  p_entity: object
    ):
        """
        Генерирует общий скрипт DDL для всех объектов сущности

        :param p_source_table: таблица источник
        :param p_idmap: idmap сущности
        :param p_anchor: якорь cущности
        :param p_attribute_table: таблица атрибут сущности
        :param p_tie: связь сущности
        :param p_entity: сущность
        """
        l_ddl="" # список со всеми DDL объектов сущности

        # формируем скрипт для таблиц источников
        for i_source_table in p_source_table: # таблиц источников у сущности может быть несколько
            # сперва удаление таблицы источника (если существует)
            l_ddl+=drop_table_ddl(p_table=i_source_table)+"\n"
            # создание таблицы источника
            l_ddl+=create_table_ddl(p_table=i_source_table)+"\n"
            l_ddl+=create_view_ddl(p_table=i_source_table)+"\n"
        # формируем скрипт для idmap
        l_ddl+=create_table_ddl(p_table=p_idmap)+"\n"
        l_ddl+=create_view_ddl(p_table=p_idmap)+"\n"
        # формируем скрипт для anchor
        l_ddl+=create_table_ddl(p_table=p_anchor)+"\n"
        l_ddl+=create_view_ddl(p_table=p_anchor)+"\n"
        # формируем скрипт для attribute
        for i_attribute_table in p_attribute_table: # может быть несколько
            l_ddl+=create_table_ddl(p_table=i_attribute_table)+"\n"
            l_ddl+=create_view_ddl(p_table=i_attribute_table)+"\n"
        # формируем скрипт для tie
        for i_tie in p_tie:  # может быть несколько
            l_ddl+=create_table_ddl(p_table=i_tie)+"\n"
            l_ddl+=create_view_ddl(p_table=i_tie)+"\n"
        # формируем скрипт для функции - конструктора запросов для сущности
        l_ddl+=p_entity.get_entity_function()
        return l_ddl

    def __ddl_checker(self, p_ddl: str):
        """
        Проверка DDL сущности

        :param p_ddl: DDL сущности
        """
        # конкретный текст ошибки
        l_result=Connection().sql_exec(p_sql=p_ddl, p_rollback=1, p_result=0) # запускаем ddl с обязательным откатом
        if l_result[1]:
            sys.exit("В сформированном DDL ошибка \n"+str(l_result[1]))


    def __metadata_checker(self,
                           p_entity: object,
                           p_source_table: list,
                           p_idmap: object,
                           p_anchor: object,
                           p_attribute_table: list,
                           p_tie: list):
        """
        Проверяет корректнось метаданных

        :param p_entity: сущность
        :param p_source_table: таблицы источники
        :param p_idmap: idmap таблица
        :param p_anchor: якорь сущности
        :param p_attribute_table: таблицы атрибуты
        :param p_tie: таблицы связи
        """
        # метаданные сущности и ее атрибутов
        p_entity.metadata_object.attrs_checker()
        for i_entity_attribute in p_entity.entity_attribute:
            i_entity_attribute.metadata_object.attrs_checker()
        # таблицы источники и их атрибуты
        for i_source_table in p_source_table:
            i_source_table.metadata_object.attrs_checker()
            for i_source_attribute in i_source_table.source_attribute:
                i_source_attribute.metadata_object.attrs_checker()
        # idmap и его атрибуты
        p_idmap.metadata_object.attrs_checker()
        for i_idmap_attribute in p_idmap.idmap_attribute:
            i_idmap_attribute.metadata_object.attrs_checker()
        # якорь и его атрибуты
        p_anchor.metadata_object.attrs_checker()
        for i_anchor_attribute in p_anchor.anchor_attribute:
            i_anchor_attribute.metadata_object.attrs_checker()
        # таблицы атрибуты и их атрибуты
        for i_attribute_table in p_attribute_table:
            i_attribute_table.metadata_object.attrs_checker()
            for i_attribute_table_attribute in i_attribute_table.attribute_table_attribute:
                i_attribute_table_attribute.metadata_object.attrs_checker()
        # таблицы связи и их атрибуты
        for i_tie in p_tie:
            i_tie.metadata_object.attrs_checker()
            for i_tie_attribute in i_tie.tie_attribute:
                i_tie_attribute.metadata_object.attrs_checker()

    def __create_metadata(self,
                          p_entity: object,
                          p_source_table: list,
                          p_idmap: object,
                          p_anchor: object,
                          p_attribute_table: list,
                          p_tie: list):
        """
        Записывает метаданные

        :param p_entity: сущность
        :param p_source_table: таблицы источники
        :param p_idmap: idmap таблица
        :param p_anchor: якорь сущности
        :param p_attribute_table: таблицы атрибуты
        :param p_tie: таблицы связи
        """
        # метаданные сущности и ее атрибутов
        p_entity.create_metadata()
        for i_entity_attribute in p_entity.entity_attribute:
            i_entity_attribute.create_metadata()
        # таблицы источники и их атрибуты
        for i_source_table in p_source_table:
            # сперва удаляем таблицу источник из метеданных (так как она могла существовать ранее)
            i_source_table.delete_metadata()
            i_source_table.create_metadata()
            for i_source_attribute in i_source_table.source_attribute:
                # сперва удаляем атрибуты таблицы источника
                i_source_attribute.delete_metadata()
                i_source_attribute.create_metadata()
        # idmap и его атрибуты
        p_idmap.create_metadata()
        for i_idmap_attribute in p_idmap.idmap_attribute:
            i_idmap_attribute.create_metadata()
        # якорь и его атрибуты
        p_anchor.create_metadata()
        for i_anchor_attribute in p_anchor.anchor_attribute:
            i_anchor_attribute.create_metadata()
        # таблицы атрибуты и их атрибуты
        for i_attribute_table in p_attribute_table:
            i_attribute_table.create_metadata()
            for i_attribute_table_attribute in i_attribute_table.attribute_table_attribute:
                i_attribute_table_attribute.create_metadata()
        # таблицы связи и их атрибуты
        for i_tie in p_tie:
            i_tie.create_metadata()
            for i_tie_attribute in i_tie.tie_attribute:
                i_tie_attribute.create_metadata()

    def __create_ddl(self, p_ddl: str):
        """
        Создает объекты в ХД

        :param p_ddl: строка в DDL объектов
        """
        Connection().sql_exec(p_sql=p_ddl,p_result=0)




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
                l_source_id=i_source.get(C_SOURCE)
                l_source_table=i_source.get(C_TABLE)
                l_source_schema=i_source.get(C_SCHEMA)
                l_source_attribute=i_source.get(C_COLUMN)
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
                 p_id: str =None,
                 p_name: str =None,
                 p_attribute_param: list =None,
                 p_desc: str =None
    ):
        """
        Конструктор

        :param p_id: id сущности
        :param p_name: наименование сущности
        :param p_attribute: атрибуты сущности
        :param p_desc: описание сущности
        """

        self._name=p_name
        self._attribute_param=p_attribute_param
        self._desc=p_desc
        self._id=p_id

    @property
    def id(self) -> str:
        """
        Id сущности
        """
        return self._id


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
        return self.attribute_checker()

    @property
    def desc(self):
        """
        Описание сущности
        """
        return self._desc

    def entity_creation_checker(self):
        """
        Проверка параметров сущности
        """
        self.name_checker()
        self.attribute_checker()
        self.source_table_pk_checker()
        self.attribute_double_checker()
        self.entity_name_double_checker()

    def name_checker(self):
        """
        Проверяет корректность наименования сущности
        """
        if not self._name:
            sys.exit("У сущности не указано наименование")

    def attribute_checker(self):
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
                        p_name=i_attribute.get(C_NAME),
                        p_desc=i_attribute.get(C_DESC),
                        p_pk=i_attribute.get(C_PK),
                        p_datatype=i_attribute.get(C_DATATYPE),
                        p_length=i_attribute.get(C_LENGTH),
                        p_scale=i_attribute.get(C_SCALE),
                        p_link_entity_id=i_attribute.get(C_LINK_ENTITY),
                        p_source_param=i_attribute.get(C_SOURCE)
                    )
                )
                l_pk_cnt=l_pk_cnt+i_attribute.get(C_PK)
            if l_pk_cnt==0:
                sys.exit("Не указано ни одного первичного ключа у сущности")

        return l_attribute

    def source_table_pk_checker(self):
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

    def attribute_double_checker(self):
            """
            Проверка на дублирующийся атрибут сущности
            """
            l_attribute_list=[]
            for i_attribute in self.attribute_param:
                l_attribute_list.append(i_attribute.name)
                l_attribute_cnt=Counter(l_attribute_list).get(i_attribute.name)
                if l_attribute_cnt>1:
                    sys.exit("Атрибут "+i_attribute.name+" указан больше одного раза")

    def entity_name_double_checker(self):
        """
        Проверяет, что в метаданных нет сущности с таким наименованием
        """
        l_entity_meta_obj=search_object(
            p_type=C_ENTITY,
            p_attrs={
                C_NAME:self.name
            }
        )
        if l_entity_meta_obj.__len__()>0:
            sys.exit("Сущность с таким наименование уже существует")













