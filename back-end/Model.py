from Metadata import *
import Source
import json
from DWH import *
from collections import Counter
from Constants import *
import SystemObjects
from SystemObjects import *

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
            AbaseError(p_error_text="JSON is empty", p_module="Model", p_class="Model",
                       p_def="__json_checker").raise_error()

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


    def create_entity(self):
        """
        Создает модель данных
        """
        # переменные для последующего использования
        l_source_table_name_list=[] # лист с уникальными наименованиями таблиц источников
        l_source_table_meta=search_object(p_type=C_QUEUE)
        if l_source_table_meta.__len__()>0:
            for i_source_table in l_source_table_meta:
                l_source_table_name_list.append(i_source_table.attrs.get(C_NAME))
        l_source_table_meta=search_object(p_type=C_QUEUE)
        if l_source_table_meta.__len__()>0:
            for i_source_table in l_source_table_meta:
                l_source_table_name_list.append(i_source_table.attrs.get(C_NAME))
        l_source_table_list=[] # лист объектов класса SourceTable - таблицы источники
        l_idmap_source_attribute_list=[] # лист атрибутов таблиц источников - первичные ключи
        l_attribute_table_list=[] # список таблиц атрибутов сущности
        #########
        # Блок обработки параметров модели и создание объектов
        ########
        # создаем сущность
        l_entity=self.__create_entity()
        # создаем якорную таблицу
        self.__create_anchor(p_entity=l_entity)
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
                # таблица источник
                l_source_table=self.__create_source_table(p_entity=l_entity, p_source_param=i_source_param)
                if l_source_table_list.__len__():
                    for index, i_source_table in enumerate(l_source_table_list):
                        if i_source_table.queue_name==l_source_table.queue_name:
                            l_source_table=i_source_table
                            break
                        elif index==l_source_table_list.__len__()-1: # дошли до конца списка
                            l_source_table_list.append(l_source_table)
                else:
                    l_source_table_list.append(l_source_table)

                # атрибут таблицы источника
                l_source_attribute=self.__create_source_attribute(
                    p_entity_attribute=l_entity_attribute,
                    p_source_table=l_source_table,
                    p_source_param=i_source_param
                )
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
            if i_attribute_param.link_entity_id:
                self.__create_tie(
                    p_entity_attribute=l_entity_attribute,
                    p_link_entity_id=i_attribute_param.link_entity_id,
                    p_source_table=l_attribute_source_table_list
                )
        #########
        # Окончание блока обработки атрибутов
        ########
        # создаем idmap
        self.__create_idmap(
            p_entity=l_entity,
            p_source_attribute=l_idmap_source_attribute_list
        )
        #########
        # Окончание блока обработки параметров модели и создание объектов
        ########

        # создаем лист из всех объектов, которые нужно создать в ХД
        l_obj=self.__get_object_list(p_entity_child=l_entity, p_source_table=l_source_table_list)
        ########
        # Блок создания DDL и ETL
        #######
        # формируем ddl
        l_ddl=self.__get_create_table_sql(p_objects=l_obj) # таблицы
        l_ddl+=self.__get_create_view_sql(p_objects=l_obj) # представления
        l_ddl+=l_entity.get_entity_function()
        # проверяем ddl
        self.__ddl_checker(p_ddl=l_ddl)
        # заново формируем список объектов, добавляя туда атрибуты и сущность
        l_obj=self.__get_object_list(
            p_entity=l_entity,
            p_entity_child=l_entity,
            p_source_table=l_source_table_list,
            p_attribute=1
        )
        # проверяем метаданные
        self.__metadata_checker(
            p_objects=l_obj
        )
        # вставляем метаданные
        self.__create_metadata(
            p_objects=l_obj
        )
        # создаем таблицы и представления в ХД
        self.__ddl_execute(p_ddl=l_ddl)

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
        # находим tie, где переименованная сущность - связанная
        l_link_tie_meta=meta.search_object(
            p_type=C_TIE,
            p_attrs={C_LINK_ENTITY:self.entity_param.id}
        )
        l_link_ties=None
        if l_link_tie_meta.__len__()>0:
            l_link_ties=[]
            for i_tie in l_link_tie_meta:
                l_link_tie=Tie(p_id=str(i_tie.uuid))
                l_link_ties.append(l_link_tie)
        # создаем лист из всех объектов, для которых нужно переделать представления
        l_obj=self.__get_object_list(
            p_entity_child=l_entity,
            p_tie=l_link_ties
        )
        # формируем скрипт удаления всех представлений сущности
        l_sql=self.__get_drop_view_sql(p_objects=l_obj)
        # удаление функции-конструктора запросов
        l_sql+=l_entity.get_drop_entity_function_sql()+"\n"
        # изменяем имя у сущности, а также у таблиц и ее атрибутов
        l_entity.name=self.entity_param.name # автоматически заменяются имена у всех производных объектов
        # отдельно заменяем наименование у tie, где переименованная сущность - связанная
        if l_link_ties:
            for i_link_tie in l_link_ties:
                i_link_tie.link_entity=l_entity

        l_obj=self.__get_object_list(
            p_entity_child=l_entity,
            p_tie=l_link_ties
        )
        # формируем скрипт создания представлений с новым наименованием
        l_sql+=self.__get_create_view_sql(p_objects=l_obj)
        # пересоздаем функцию-конструктор запросов
        l_sql+=l_entity.get_entity_function()
        # проверяем ddl
        self.__ddl_checker(p_ddl=l_sql)
        # заново формируем список объектов, добавляя туда сущность и атрибуты объектов
        l_obj=self.__get_object_list(
            p_entity=l_entity,
            p_entity_child=l_entity,
            p_tie=l_link_ties,
            p_attribute=1
        )
        # проверяем метаданные
        self.__metadata_checker(
            p_objects=l_obj
        )
        # записываем метаданные
        self.__update_metadata(
            p_objects=l_obj
        )
        # выполняем ddl
        self.__ddl_execute(p_ddl=l_sql)

    def alter_desc(self):
        """
        Изменяет описание сущности
        """
        # инициализируем объект сущности
        l_entity=Entity(
            p_id=self.entity_param.id
        )
        # изменяем у объекта описание
        l_entity.desc=self.entity_param.desc
        # проверяем метаданные
        self.__metadata_checker(p_objects=[l_entity])
        # обновляем метаданные
        self.__update_metadata(p_objects=[l_entity])

    def drop_entity(self):
        """
        Удаляет сущность
        """
        # инициализируем объект сущности
        l_entity=Entity(
            p_id=self.entity_param.id
        )
        # находим tie, где переименованная сущность - связанная
        l_link_tie_meta=meta.search_object(
            p_type=C_TIE,
            p_attrs={C_LINK_ENTITY:self.entity_param.id}
        )
        l_link_ties=None
        if l_link_tie_meta.__len__()>0:
            l_link_ties=[]
            for i_tie in l_link_tie_meta:
                l_link_tie=Tie(p_id=str(i_tie.uuid))
                l_link_ties.append(l_link_tie)
        # формируем список объектов, которые требуется удалить
        l_obj=self.__get_object_list(p_entity_child=l_entity, p_tie=l_link_ties)
        # формируем скрипт удаления объектов
        l_sql=l_entity.get_drop_entity_function_sql()+"\n"
        l_sql+=self.__get_drop_view_sql(p_objects=l_obj)
        l_sql+=self.__get_drop_table_sql(p_objects=l_obj)
        # проверяем скрипт
        self.__ddl_checker(p_ddl=l_sql)
        # формируем метаданные для удаления
        # снова формируем список объектов
        l_obj=self.__get_object_list(
            p_entity_child=l_entity,
            p_entity=l_entity,
            p_tie=l_link_ties,
            p_attribute=1
        )
        # обновляем метаданные у сущностей, которые ссылались на удаленный tie (l_link_tie)
        if l_link_ties:
            for i_link_tie in l_link_ties:
                l_link_entity=i_link_tie.entity
                l_tie_list=[]
                for i_tie in l_link_entity.tie:
                    if i_tie.id!=i_link_tie.id: # формируем лист tie сущности без удаленного
                        l_tie_list.append(i_tie)
                l_link_entity.tie=l_tie_list # заменяем лист объектов у сущности
                # обновляем метаданные
                self.__update_metadata(p_objects=[l_link_entity])
        # удаляем метаданные
        self.__delete_metadata(p_objects=l_obj)
        #удаляем объекты из ХД
        self.__ddl_execute(p_ddl=l_sql)
        #TODO: вынести проверку метаданные и ddl, а также их запуск в отедльный метод


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
        # по наименованию будем искать в метаданных
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
        # проверяем, была ли добавлена таблица источник в сущность
        l_entity_source_table_name_list=[]
        if p_entity.source_table:
            for i_source_table in p_entity.source_table:
                l_entity_source_table_name_list.append(i_source_table.queue_name)
            if l_source_table.queue_name in l_entity_source_table_name_list:
                return l_source_table
        # добавляем таблицу источник в сущность, если ранее не была добавлена
        add_table(p_object=p_entity, p_table=l_source_table)
        # добавляем id источника в сущность
        add_source_to_object(p_object=p_entity, p_source=l_source_table.source)
        return l_source_table

    def __create_source_attribute(self,
                                  p_entity_attribute: object,
                                  p_source_table: object,
                                  p_source_param: object
    ):
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

        # добавяем атрибут к метаданным атрибута сущности - в любом случае
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

        # вытаскиваем rk
        l_rk=None
        for i_attr in l_anchor.anchor_attribute:
            if i_attr.attribute_type==C_RK:
                l_rk=i_attr

        l_entity_rk=Attribute( # атрибут rk для добавления в метаданные сущности
            p_name=l_rk.name,
            p_desc=C_RK_DESC,
            p_datatype=l_rk.datatype.data_type_name,
            p_length=l_rk.datatype.data_type_length,
            p_scale=l_rk.datatype.data_type_scale,
            p_type=C_ENTITY_COLUMN,
            p_rk=1,
            p_anchor=l_anchor
        )
        add_attribute(p_table=p_entity, p_attribute=l_entity_rk) # добавляем в сущность

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
        p_entity_attribute.attribute_table=l_attribute_table
        return l_attribute_table

    def __create_tie(self,
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
            p_entity=p_entity_attribute.entity,
            p_entity_attribute=p_entity_attribute,
            p_link_entity=l_link_entity,
            p_source_table=p_source_table
        )
        l_link_rk=None
        # вытаскиваем link_rk атрибут
        for i_attr in l_tie.tie_attribute:
            if i_attr.attribute_type==C_LINK_RK:
                l_link_rk=i_attr

        l_entity_link_rk=Attribute( # создаем атрибут для добавления в метаданные сущности
            p_name=l_link_rk.name,
            p_desc=C_LINK_RK_DESC,
            p_datatype=l_link_rk.datatype.data_type_name,
            p_length=l_link_rk.datatype.data_type_length,
            p_scale=l_link_rk.datatype.data_type_scale,
            p_type=C_ENTITY_COLUMN,
            p_link_entity=l_link_entity,
            p_fk=1,
            p_tie=l_tie
        )
        add_attribute(p_table=p_entity_attribute.entity, p_attribute=l_entity_link_rk)

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

    def __get_object_list(
            self,
            p_entity: object =None,
            p_entity_child: object =None,
            p_idmap: list =None,
            p_anchor: list =None,
            p_attribute_table: list =None,
            p_tie: list =None,
            p_source_table: list =None, 
            p_attribute: int =0
    ):
        """
        Собирает в список указанные объекты
        Если указана сущность (p_entity_child), будут собираться все ее дочерние объекты.

        :param p_entity: сущность
        :param p_entity_child: сущность, для которой нужно добавить дочерние объекты
        :param p_idmap: idmap (лист объектов)
        :param p_anchor: якорная таблица (лист объектов)
        :param p_attribute_table: таблица атрибута (лист объектов)
        :param p_tie: таблица связи (лист объектов)
        :param p_source_table: таблица источник (лист объектов)
        :param p_attribute: признак, что также нужно добавить атрибуты объектов (по-дефолту 0)
        """
        l_obj=[] # лист объектов, для которых требуется сформировать скрипт

        if p_source_table:
            l_obj.extend(p_source_table)
            if p_attribute==1:
                for i_table in p_source_table:
                    l_obj.extend(i_table.source_attribute)
        if p_anchor:
            l_obj.extend(p_anchor)
            if p_attribute==1:
                for i_table in p_anchor:
                    l_obj.extend(i_table.anchor_attribute)
        if p_attribute_table:
            l_obj.extend(p_attribute_table)
            if p_attribute==1:
                for i_table in p_attribute_table:
                    l_obj.extend(i_table.attribute_table_attribute)
        if p_tie:
            l_obj.extend(p_tie)
            if p_attribute==1:
                for i_table in p_tie:
                    l_obj.extend(i_table.tie_attribute)
        if p_idmap:
            l_obj.extend(p_idmap)
            if p_attribute==1:
                for i_table in p_idmap:
                    l_obj.extend(i_table.idmap_attribute)
        if p_entity_child: # если указана сущность - из нее вытаскиваются все дочерние объекты
            l_obj.extend(
                [
                    p_entity_child.anchor,
                    p_entity_child.idmap
                ]
            )
            if p_attribute==1:
                l_obj.extend(p_entity_child.anchor.anchor_attribute)
                l_obj.extend(p_entity_child.idmap.idmap_attribute)
            for i_attribute_table in p_entity_child.attribute_table:
                l_obj.append(i_attribute_table)
                if p_attribute==1:
                    l_obj.extend(i_attribute_table.attribute_table_attribute)
            if p_entity_child.tie:
                for i_tie in p_entity_child.tie:
                    l_obj.append(i_tie)
                    if p_attribute==1:
                        l_obj.extend(i_tie.tie_attribute)
        if p_entity:
            l_obj.append(p_entity)
            if p_attribute==1:
                l_obj.extend(p_entity.entity_attribute)
        return l_obj

    def __get_create_table_sql(self, p_objects: list) -> str:
        """
        Генерирует SQL по созданию таблиц

        :param p_objects: лист объектов, для которых требуется сформировать SQL
        """
        l_sql=""
        for i_obj in p_objects:
            if i_obj.type==C_QUEUE: # если объект - queue-таблица, ее сперва удаляем
                l_sql+=drop_table_ddl(p_table=i_obj)+"\n"
            l_sql+=create_table_ddl(p_table=i_obj)+"\n"

        return l_sql

    def __get_create_view_sql(self, p_objects: list) -> str:
        """
        Генерирует SQL по созданию представлений

        :param p_objects: лист объектов, для которых требуется сформировать SQL
        """
        l_sql=""
        for i_obj in p_objects:
            if i_obj.type==C_QUEUE: # если объект - queue-таблица, ее сперва удаляем
                l_sql+=drop_view_ddl(p_table=i_obj)+"\n"
            l_sql+=create_view_ddl(p_table=i_obj)+"\n"

        return l_sql

    def __get_drop_table_sql(self, p_objects: list) -> str:
        """
        Генерирует SQL по удалению таблиц

        :param p_objects: лист объектов, для которых требуется сформировать SQL
        """
        l_sql=""
        for i_obj in p_objects:
            l_sql+=drop_table_ddl(p_table=i_obj)+"\n"

        return l_sql

    def __get_drop_view_sql(self, p_objects: list) -> str:
        """
        Генерирует SQL по удалению представлений

        :param p_objects: лист объектов, для которых требуется сформировать SQL
        """
        l_sql=""
        for i_obj in p_objects:
            l_sql+=drop_view_ddl(p_table=i_obj)+"\n"

        return l_sql

    def __ddl_checker(self, p_ddl: str):
        """
        Проверка DDL сущности

        :param p_ddl: DDL сущности
        """
        # конкретный текст ошибки
        l_result=Connection().sql_exec(p_sql=p_ddl, p_rollback=1, p_result=0) # запускаем ddl с обязательным откатом
        if l_result[1]:
            AbaseError(p_error_text="Created DDL is not correct \n"+str(l_result[1]), p_module="Model", p_class="Model",
                       p_def="__ddl_checker").raise_error()


    def __metadata_checker(self,p_objects: list):
        """
        Проверяет корректнось метаданных

        :param p_objects: лист объектов, для которых требуется проверить метаданные
        """
        # метаданные сущности и ее атрибутов
        for i_obj in p_objects:
            i_obj.metadata_object(p_id=i_obj.id, p_attrs=i_obj.metadata_json).attrs_checker()

    def __create_metadata(self,p_objects: list):
        """
        Записывает метаданные

        :param p_objects: лист объектов, которым требуется записать метаданные
        """
        # метаданные сущности и ее атрибутов
        for i_obj in p_objects:
            i_obj.create_metadata()

    def __update_metadata(self,p_objects: list):
        """
        Обновляет метаданные

        :param p_objects: лист объектов, для которых требуется перезаписать метаданные
        """
        # метаданные сущности и ее атрибутов
        for i_obj in p_objects:
            i_obj.update_metadata()

    def __delete_metadata(self,p_objects: list):
        """
        Удаляет метаданные

        :param p_objects: лист объектов, для которых требуется удалить метаданные
        """
        # метаданные сущности и ее атрибутов
        for i_obj in p_objects:
            i_obj.delete_metadata()

    def __ddl_execute(self, p_ddl: str):
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
            AbaseError(p_error_text="Source value is empty", p_module="Model", p_class="Model",
                       p_def="__source_id_checker").raise_error()
        # проверка на корректное заполнение - uuid
        try:
            uuid.UUID(self.source_id)
        except ValueError as e:
            AbaseError(p_error_text="ID of source is incorrect", p_module="Model", p_class="Model",
                       p_def="__source_id_checker").raise_error()
        # проверка на существование в метаданных источника (просто инициализируем объект)
        l_source_meta=Source(p_id=self.source_id)

    def __table_checker(self):
        """
        Проверяет таблицу источника
        """
        if not self.table:
            AbaseError(p_error_text="Table of source is empty", p_module="Model", p_class="Model",
                       p_def="__table_checker").raise_error()

    def __column_checker(self):
        """
        Проверяет столбец источника
        :return:
        """
        if not self.column:
            AbaseError(p_error_text="Column of source is empty", p_module="Model", p_class="Model",
                       p_def="__column_checker").raise_error()

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
            AbaseError(p_error_text="Name of attribute is empty", p_module="Model", p_class="_AttributeParam",
                       p_def="__name_checker").raise_error()

    def __datatype_checker(self):
        """
        Проверяет правильность типа данных
        """
        if not self.datatype:
            AbaseError(p_error_text="Type of attribute is missed", p_module="Model", p_class="_AttributeParam",
                       p_def="__datatype_checker").raise_error()

    def __link_entity_checker(self):
        """
        Проверяет корректность заданной связанной сущности
        """
        if self.link_entity_id:
            try:
                uuid.UUID(self.link_entity_id)
            except ValueError as e:
                AbaseError(p_error_text="ID of linked entity is incorrect", p_module="Model", p_class="_AttributeParam",
                           p_def="__link_entity_checker").raise_error()
            # проверка на существование сущности в метаданных источника (просто инициализируем объект)
            l_entity_meta=Entity(p_id=self.link_entity_id)

    def __source_checker(self):
        """
        Проверяет корректность источника атрибута
        """
        if not self._source_param:
            AbaseError(p_error_text="Attribute doesn't have any source", p_module="Model", p_class="_AttributeParam",
                       p_def="__source_checker").raise_error()
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
                    AbaseError(p_error_text="Source "+str(l_source_table)+" is mentioned two times "+self._name, p_module="Model",
                               p_class="_AttributeParam", p_def="__source_checker").raise_error()
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
            AbaseError(p_error_text="Incorrect value of PK", p_module="Model",
                       p_class="_AttributeParam", p_def="__pk_checker").raise_error()
        elif self._pk>1:
            AbaseError(p_error_text="Incorrect value of PK", p_module="Model",
                       p_class="_AttributeParam", p_def="__pk_checker").raise_error()

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
        self.source_attribute_double_checker()
        self.link_entity_double_checker()

    def name_checker(self):
        """
        Проверяет корректность наименования сущности
        """
        if not self._name:
            AbaseError(p_error_text="Name of entity is missed", p_module="Model",
                       p_class="_EntityParam", p_def="name_checker").raise_error()

    def attribute_checker(self):
        """
        Проверка параметров атрибутов
        """
        l_attribute=[]
        if not self._attribute_param:
            AbaseError(p_error_text="Entity doesn't have any attribute", p_module="Model",
                       p_class="_EntityParam", p_def="attribute_checker").raise_error()
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
                AbaseError(p_error_text="PK of entity wasn't mentioned", p_module="Model",
                           p_class="_EntityParam", p_def="attribute_checker").raise_error()

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
                AbaseError(p_error_text="Source table "+i_source_table+" isn't mentioned like source of PK", p_module="Model",
                           p_class="_EntityParam", p_def="source_table_pk_checker").raise_error()

    def attribute_double_checker(self):
            """
            Проверка на дублирующийся атрибут сущности
            """
            l_attribute_list=[]
            for i_attribute in self.attribute_param:
                l_attribute_list.append(i_attribute.name)
                l_attribute_cnt=Counter(l_attribute_list).get(i_attribute.name)
                if l_attribute_cnt>1:
                    AbaseError(p_error_text="Attribute "+i_attribute.name+" is mentioned more than one time", p_module="Model",
                               p_class="_EntityParam", p_def="attribute_double_checker").raise_error()

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
            AbaseError(p_error_text="Entity with the name "+self.name+" already exists",
                       p_module="Model", p_class="_EntityParam", p_def="entity_name_double_checker").raise_error()

    def source_attribute_double_checker(self):
        """
        Проверяет, что один и тот же атрибут источника не указан у сущности дважды
        """
        l_source_attribute_name_list=[] # наименование атрибутов с источника
        for i_attribute in self.attribute_param:
            for i_source_attribute in i_attribute.source_param:
                l_source_attribute_name=str(i_source_attribute.source_id)+"_" \
                                        +i_source_attribute.schema+"_" \
                                        +i_source_attribute.table+"_" \
                                        +i_source_attribute.column
                if l_source_attribute_name in l_source_attribute_name_list:
                    AbaseError(p_error_text="Attribute of source "+str(i_source_attribute.source_id)+" "
                             +i_source_attribute.schema+"."
                             +i_source_attribute.table+"."
                             +i_source_attribute.column+" is mentioned twice",
                               p_module="Model", p_class="_EntityParam",
                               p_def="source_attribute_double_checker").raise_error()
                else:
                    l_source_attribute_name_list.append(l_source_attribute_name)

    def link_entity_double_checker(self):
        """
        Проверяет, что связанная сущность не указана два раза у разных атрибутов сущности
        """
        l_link_entity_id_list=[]
        for i_attribute in self.attribute_param:
            if i_attribute.link_entity_id in l_link_entity_id_list:
                AbaseError(p_error_text=str(i_attribute.link_entity_id)+" is mentioned by attributes of entity more than one time" ,
                           p_module="Model", p_class="_EntityParam",
                           p_def="link_entity_double_checker").raise_error()
            elif i_attribute.link_entity_id:
                l_link_entity_id_list.append(i_attribute.link_entity_id)












