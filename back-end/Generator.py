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


class Table:

    """
    Класс по работе с таблицами
    """

    def __init__(self,
                 p_table_type: str,
                 p_table_id: str,
                 p_table_name: str,
                 p_table_attrs: list =None,
                 p_source_tables: list =None,
                 p_source_schema: str =None,
                 p_source_table_name: str =None
                 ):
        """
        Конструктор

        :param p_table_id: id таблицы в метаданных
        :param p_table_name: наименование таблицы
        :param p_table_attrs: словарь атрибутов таблицы
        :param p_table_type: тип таблицы
        :param p_source_table: Список таблиц источиков (объекты класса SourceTable)
        :param p_source_schema: Наименование схемы таблицы на источнике (заполняется только у queue таблиц)
        :param p_source_table_name: Наименование таблицы на источнике (заполняется только у queue таблиц)
        """
        self._table_id=p_table_id
        self._table_name=p_table_name
        self._table_attrs=p_table_attrs
        self._table_type=p_table_type
        self._source_tables=p_source_tables
        self._source_schema=p_source_schema
        self._source_table_name=p_source_table_name

        # определение параметров класса в соответсвии с типом таблицы
        l_table_components=const('C_TABLE_COMPONENTS').constant_value.get(self.table_type,None) # достаем компоненты таблицы в соответствии с типом
        self.table_attr_type_list=l_table_components.get(const('C_TABLE_ATTRIBUTES').constant_value)
        self._table_template_variables=l_table_components.get(const('C_TABLE_DDL_COMPONENTS').constant_value)
        self._table_etl_template_variables=l_table_components.get(const('C_TABLE_ETL_COMPONENTS').constant_value)

    @property
    def table_type(self):
        """
        Тип таблицы якорной модели
        """
        self._table_type=self._table_type.lower()
        # проверка на корректное указание типа
        if self._table_type not in const('C_TABLE_TYPE_LIST').constant_value:
            sys.exit("Не существует типа таблицы"+self._table_type) #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._table_type

    @property
    def table_id(self) -> str:
        """
        Id таблицы (приводит к нижнему регистру)
        """
        return str(self._table_id).lower()

    @property
    def table_name(self) -> str:
        """
        Наименование таблицы (приводит к нижнему регистру)
        """
        return self._table_name.lower()

    @property
    def table_attrs(self) -> list:
        """
        Атрибуты таблицы
        """
        return self._table_attrs

    @property
    def table_attrs_type(self):
        """
        Список типов атрибутов у таблицы
        """
        l_attrs_type_list=[]
        for i_table_attrs in self.table_attrs:
            l_attrs_type_list.append(i_table_attrs.attribute_type)

        return l_attrs_type_list

    def table_attr_double_checker(self):
        """
        Проверяет наличие необходимых атрибутов у таблицы в сооветствии с ее типом
        """
        l_attrs_type_set=set(self.table_attrs_type)
        if len(self.table_attrs_type)>len(l_attrs_type_set) and self.table_type!=const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
            sys.exit("У таблицы "+self.table_name+" несколько атрибутов с одинаковым типом") #TODO: реализовать вывод ошибок, как сделал Рустем

    def table_attr_type_checker(self):
        """
        Проверяет наличие всех необходимых атрибутов по их типу
        """
        for i_attr_type_list in self.table_attr_type_list:
            if i_attr_type_list not in self.table_attrs_type:
                sys.exit("У таблицы "+self.table_name+" нет атрибута с типом "+i_attr_type_list) #TODO: реализовать вывод ошибок, как сделал Рустем

    @property
    def table_template_variables(self) -> dict:
        """
        Переменные и их значения для подстановки в шаблон
        """
        # сперва проверяем, что все атрибуты у таблицы указаны корректно
        self.table_attr_type_checker()
        self.table_attr_double_checker()
        l_table_template_variables_dict={}
        # получаем все ключи списка с переменными
        l_table_template_variables_list=list(self._table_template_variables.keys())

        # подставляем в список переменных значения
        for i_table_template_variables_list in l_table_template_variables_list:
            l_queue_table_attr_ddl=""
            l_queue_view_attr_ddl=""
            for i_table_attr in self.table_attrs:
                if i_table_template_variables_list==i_table_attr.attribute_type \
                        or \
                        i_table_template_variables_list==i_table_attr.attribute_type+"_"+const('C_NAME').constant_value: # для view
                    if i_table_template_variables_list==const('C_VALUE_TYPE_NAME').constant_value:
                        l_variable_value=i_table_attr.attribute_table_ddl # у таблицы attribute в столбец value подставляем ddl атрибута
                    elif i_table_template_variables_list==const('C_NAME_VALUE_ATTRIBUTE').constant_value: # для view проставляем также наименование атрибута
                        l_variable_value=i_table_attr.attribute_view_ddl
                    elif i_table_template_variables_list==const('C_QUEUE_ATTR_TYPE_NAME').constant_value:  # для не технических атрибутов queue таблицы формируем отдельный ddl
                        l_queue_table_attr_ddl=l_queue_table_attr_ddl+i_table_attr.attribute_table_ddl+","+'\n\t'
                        l_variable_value=l_queue_table_attr_ddl
                    elif i_table_template_variables_list==const('C_NAME_QUEUE_ATTR_ATTRIBUTE').constant_value:  # для не технических атрибутов queue view формируем отдельный ddl
                        l_queue_view_attr_ddl=l_queue_view_attr_ddl+i_table_attr.attribute_view_ddl+","+'\n\t'
                        l_variable_value=l_queue_view_attr_ddl
                    elif i_table_template_variables_list!=i_table_attr.attribute_type:  #  для _name
                        l_variable_value=i_table_attr.attribute_name # проставляем наименование атрибута
                    else:
                        l_variable_value=i_table_attr.attribute_id # в остальных случаях подставляем id атрибута
                    l_table_template_variables_dict.update(
                        {
                            self._table_template_variables.get(i_table_template_variables_list,None)
                            :l_variable_value # в качестве названий атрибутов подставляется id атрибутов в метаданных
                        }
                    )

        # отдельно заменяем наименование таблицы в шаблоне
        l_table_template_variables_dict.update(
            {
                self._table_template_variables.get(const('C_TABLE').constant_value,None):self.table_id,
                self._table_template_variables.get(const('C_NAME_TABLE').constant_value,None):self.table_name
            }
        )
        return l_table_template_variables_dict

    @property
    def table_ddl(self):
        """
        Сгенерированный DDL таблицы
        """
        l_table_ddl_template=Driver.Template(
            p_table_type=self.table_type,
            p_template_type=const('C_DDL').constant_value
        )
        # заменяем в шаблоне переменные на значения
        l_table_ddl = l_table_ddl_template.template_file.replace_in_body(self.table_template_variables)
        return l_table_ddl

    @property
    def view_ddl(self):
        """
        Сгенерированный DDL представления
        """
        l_view_ddl_template=Driver.Template(
            p_table_type=self.table_type,
            p_template_type=const('C_VIEW').constant_value
        )
        # заменяем в шаблоне переменные на значения
        l_view_ddl = l_view_ddl_template.template_file.replace_in_body(self.table_template_variables)
        return l_view_ddl

    @property
    def table_etl_template_variables(self):
        """
        Шаблон ETL
        """
        return self._table_etl_template_variables

    @property
    def source_tables(self):
        """
        Таблицы источники
        """
        # проверка, что таблицы источники указаны
        if self._source_tables is None:
            sys.exit("Не указан источник у таблицы "+self.table_id)
        # проверка, что таблицы источники указаны верно в соответствии с типом таблицы
        l_source_table_type_list_purpose=list(self.table_etl_template_variables.keys())
        for i_source_table_type_list_purpose in l_source_table_type_list_purpose: # удаляем из списка все, что не относится к типам таблиц
            if i_source_table_type_list_purpose not in const('C_SOURCE_TABLE_TYPE_LIST').constant_value:
                l_source_table_type_list_purpose.remove(i_source_table_type_list_purpose)
        l_source_table_type_list=[]
        for i_source_table in self._source_tables:
            if i_source_table.source_table_type not in l_source_table_type_list_purpose:
                sys.exit("Не существует типа таблицы источника"+i_source_table.source_table_type+" для таблицы типа "+self.table_type) #TODO: реализовать вывод ошибок, как сделал Рустем
            l_source_table_type_list.append(i_source_table.source_table_type)
        # проверка, что все необходимые таблицы источники указаны
        for i_source_table_type in l_source_table_type_list_purpose:
            if i_source_table_type not in l_source_table_type_list:
                sys.exit("Указаны не все таблицы  для таблицы "+self._table_id) #TODO: реализовать вывод ошибок, как сделал Рустем

        return self._source_tables

    def attribute_id_sql(self, p_attribute_type: list=None):
        """
        Перечисление id атрибутов для insert запроса
        :param p_attribute_type: cписок типов атрибутов, которые требуется включить в перечисление
        """
        l_attribute_list_sql=""
        for i_attribute in self.table_attrs:
            l_attribute_type_list=p_attribute_type or [i_attribute.attribute_type]
            if i_attribute.attribute_type in l_attribute_type_list:
                l_attribute_list_sql=l_attribute_list_sql+'"'+str(i_attribute.attribute_id)+'"'+","
        l_attribute_list_sql=l_attribute_list_sql[:-1]
        return l_attribute_list_sql

    def attribute_name_sql(self, p_attribute_type: list=None):
        """
        Перечисление name атрибутов для insert запроса
        :param p_attribute_type: cписок типов атрибутов, которые требуется включить в перечисление

        """
        l_attribute_list_sql=""
        for i_attribute in self.table_attrs:
            l_attribute_type_list=p_attribute_type or [i_attribute.attribute_type]
            if i_attribute.attribute_type in l_attribute_type_list:
                l_attribute_list_sql=l_attribute_list_sql+'"'+str(i_attribute.attribute_name)+'"'+","
        l_attribute_list_sql=l_attribute_list_sql[:-1]
        return l_attribute_list_sql

    @property
    def cast_attribute_sql(self, p_attribute_type: list=None):
        """
        Перечисление атрибутов таблицы с cast для insert. Заместо  id атрибута вставляет порядковый номер в запросе к источнику
        :param p_attribute_type: cписок типов атрибутов, которые требуется включить в перечисление

        """
        l_attribute_cast_sql="("
        l_num=0
        for i_attribute in self.table_attrs:
            l_attribute_cast_sql=l_attribute_cast_sql+i_attribute.cast_attribute_script(p_attribute_name=l_num,p_datatype=None)+","
            l_num=l_num+1
        l_attribute_cast_sql=l_attribute_cast_sql[:-1]+")"
        return l_attribute_cast_sql

    @property
    def source_schema(self):
        """
        Схема таблицы на источнике
        """
        if self._source_schema is None:
            sys.exit("Не указана схема таблицы источника у таблицы "+self._table_id) #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._source_schema

    @property
    def source_table_name(self):
        """
        Наименование таблицы на источнике
        """
        if self._source_table_name is None:
            sys.exit("Не указано наименование таблицы источника у таблицы "+self._table_id) #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._source_table_name




class Attribute:
    """
    Атрибут таблицы
    """

    def __init__(self,
                 p_attribute_type: str,
                 p_attribute_name: str,
                 p_attribute_id: str,
                 p_data_type: dict
                 ):
        """
        Конструктор

        :param p_attribute_type: тип атрибута
        :param p_attribute_name: наименование атрибута
        :param p_data_type: тип данных
        """
        self._attribute_type=p_attribute_type
        self._attribute_id=p_attribute_id
        self._attribute_name=p_attribute_name
        self._data_type=p_data_type

    @property
    def attribute_type(self) -> str:
        """
        Тип атрибута
        Проверка на корректное значение
        Нижний регистр
        """
        if self._attribute_type not in const('C_ATTRIBUTE_TABLE_TYPE_LIST').constant_value:
            sys.exit("У атрибута указан неправильный тип "+self._attribute_type) #TODO: реализовать вывод ошибок, как сделал Рустем
        elif self._attribute_type is None:
            sys.exit("У атрибута "+self.attribute_name+" не указан тип ") #TODO: реализовать вывод ошибок, как сделал Рустем

        return self._attribute_type.lower()


    @property
    def attribute_id(self) -> str:
        """
        Id атрибута
        Нижний регистр
        """
        if self._attribute_id is None:
            sys.exit("У атрибута "+self._attribute_name+" не указан id ") #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._attribute_id.lower()

    @property
    def attribute_name(self):
        """
        Наименование атрибута
        Нижний регистр
        """
        if self._attribute_name is None:
            sys.exit("У атрибута "+self._attribute_id+" не указан name ") #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._attribute_name.lower()

    @property
    def data_type(self):
        """
        Тип данных
        Проверка на корректное значение
        """
        if self._data_type is None:
            sys.exit("У атрибута "+self.attribute_name+" не указан тип данных") #TODO: реализовать вывод ошибок, как сделал Рустем

        l_data_type_name=self._data_type.get(const('C_DATATYPE').constant_value,None)
        l_data_type_length=self._data_type.get(const('C_LENGTH').constant_value,None)
        l_data_type_scale=self._data_type.get(const('C_SCALE').constant_value,None)
        l_data_type=Driver.DataType(
            p_data_type_name=l_data_type_name,
            p_data_type_length=l_data_type_length,
            p_data_type_scale=l_data_type_scale
        )
        # l_data_type_dict = {
        #     const('C_DATATYPE').constant_value:l_data_type.data_type_name,
        #     const('C_LENGTH').constant_value:l_data_type.data_type_length,
        #     const('C_SCALE').constant_value:l_data_type.data_type_scale
        # }
        return l_data_type


    @property
    def data_type_ddl(self):
        """
        DDL типа данных
        """
        return self.data_type.data_type_sql

    @property
    def attribute_table_ddl(self):
        """
        DDL атрибута в таблице
        """
        l_attribute_ddl='"'+self.attribute_id+'"'+" "+self.data_type_ddl
        return l_attribute_ddl

    @property
    def attribute_view_ddl(self):
        """
        DDL атрибута во view
        """
        l_attribute_ddl='"'+self.attribute_id+'"'+" AS "+'"'+self.attribute_name+'"'
        return l_attribute_ddl

    def cast_attribute_script(self,p_datatype: object =None, p_attribute_name: str =None):
        """
        Делает конструкцию CAST(attribute_id as datatype)

        :param p_datatype: новый тип данных для атрибута (объект класса DataType)
        :param p_attribute_name: алиас атрибута
        """
        l_data_type_ddl=""
        if p_datatype is not None and type(p_datatype).__name__=="DataType":
            l_data_type_ddl=p_datatype.data_type_sql
        else:
            l_data_type_ddl=self.data_type_ddl
        l_attribute_name=""
        if p_attribute_name is not None:
            l_attribute_name=str(p_attribute_name)
        else:
            l_attribute_name=self.attribute_id
        l_cast_attribute_script=const('C_CAST').constant_value+"("+'"'+l_attribute_name+'"'+" AS "+l_data_type_ddl+")"
        return l_cast_attribute_script

class SourceTable:
    """
    Таблица источник
    """

    def __init__(self,
                 p_table: object,
                 p_source_table_type: str,
                 p_source_table_attr: list =None,
                 p_source_id: str =None
                 ):
        """
        Конструктор

        :param p_table: Объект класса Table
        :param p_source_table_type: Тип таблицы источника
        :param p_table_attr: Атрибуты таблицы источника
        :param p_source_id: Id источника (заполняется у queue таблиц)
        """
        self._table=p_table
        self._source_table_type=p_source_table_type
        self._source_table_attr=p_source_table_attr
        self._source_id=p_source_id

    @property
    def table(self) -> object:
        """
        Объект класса Table
        """
        # проверяем, что объект класса именно Table
        if type(self._table).__name__!="Table":
            sys.exit("Параметр p_table не объект класса Table") #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._table

    @property
    def source_table_type(self) -> str:
        """
        Тип таблицы источника
        """
        # проверяем, что указан корректный тип
        l_source_table_type=self._source_table_type.lower()
        if l_source_table_type not in const('C_SOURCE_TABLE_TYPE_LIST').constant_value:
            sys.exit("Не существует заданного типа таблицы источника "+l_source_table_type) #TODO: реализовать вывод ошибок, как сделал Рустем
        return l_source_table_type

    @property
    def source_table_attr(self) -> list:
        """
        Атрибуты таблицы источника
        """
        return self._source_table_attr

    @property
    def source_id(self) -> str:
        """
        Id источника (заполняется только у queue таблиц)
        """
        # проверка, что у queue таблицы заполнен source_id
        if self.source_table_type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value and self._source_id is None:
            sys.exit("У queue таблицы не заполнен источник") #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._source_id

class SourceAttribute:
    """
    Атрибуты таблицы источника
    """

    def __init__(self, p_attribute: object, p_source_attribute_type: str):
        """
        Конструктор

        :param p_attribute: объект класса Attribute
        :param p_source_attribute_type: тип атрибута источника
        """
        self._attribute=p_attribute
        self._source_attribute_type=p_source_attribute_type

    @property
    def attribute(self) -> object:
        """
        Объект класса Attribute
        :return:
        """
        # проверка, что передает объект класса Attibute
        if type(self._attribute).__name__!="Attribute":
            sys.exit("Параметр p_attribute не объект класса Attibute") #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._attribute

    @property
    def source_attribute_type(self):
        """
        Тип атрибута источника
        """
        # проверка, что указан корректный тип атрибута
        l_source_attribute_type=self._source_attribute_type.lower()
        if self._source_attribute_type not in const('C_SOURCE_ATTRIBUTE_TABLE_TYPE_LIST').constant_value:
            sys.exit("Не существует заданного типа атрибута источника "+l_source_attribute_type) #TODO: реализовать вывод ошибок, как сделал Рустем
        return l_source_attribute_type


class ETL:
    """
    ETL-процедура
    """

    def __init__(self, p_table: object):
        """
        Конструктор

        :param p_table: Объект класса Table, для которого нужно сформировать ETL
        """
        self._table=p_table

    @property
    def table(self) -> object:
        """
        Таблица (объект класса Table)
        """
        # проверка, что объект действительно класса Table
        if type(self._table).__name__!="Table":
            sys.exit("Переменная p_table не объект класса Table") #TODO: реализовать вывод ошибок, как сделал Рустем
        return self._table

    def get_temp_table_name(self, p_queue_table_id: str, p_temp_table_num: int):
        """
        Формирует наименование временной таблицы для использования в etl

        :param p_queue_table_id: id queue таблицы
        :param p_temp_table_num: порядковый номер временной таблицы
        """
        return Metadata.Metadata.generate_guid()


    def get_nk_cast_sql(self,p_nk_list):
        """
        Формирует SQL-выражение CAST(... AS ...)

        :param p_nk_list: список натуральных ключей
        """
        l_nk_sql_list=[] # sql-выражение
        l_data_type = Driver.DataType( # для конкатенации используется varchar(1000)
            p_data_type_name="varchar",
            p_data_type_length=1000
        )
        p_nk_list.sort()
        for i_nk in p_nk_list:
            l_nk_sql_list.append(const('C_CAST').constant_value+"("+'"'+i_nk+'"'+" AS "+l_data_type.data_type_sql+")")
        return l_nk_sql_list

    def get_concat_script(self, p_nk_list: list):
        """
        Формирует скрипт конкатенации

        :param p_nk_list: лист из id натуральных ключей
        """
        l_nk_cast_list=self.get_nk_cast_sql(p_nk_list)
        l_concat_script=""
        l_concat_str="||"+"'"+const('C_CONCAT_SYMBOL').constant_value+"'"+"||"
        for i_attribute_cast_script in l_nk_cast_list[:-1]:
            l_concat_script=l_concat_script+i_attribute_cast_script+l_concat_str
        return l_concat_script+l_nk_cast_list[-1]

    def get_all_etl_templates(self):
        """
        Выдает лист etl-шаблонов для таблицы (формирует для каждой queue таблицы отдельный шаблон)
        """
        l_etl_templates = [] # лист словарей с переменными и подставленными значениями
        l_etl = [] # лист etl процедур
        l_queue_source_table_list = []
        if self.table.table_type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
            l_etl_templates.append(self.get_etl_template(p_source_tables=[])) # у таблицы queue нет источников
            return l_etl_templates
        for i_source_table in self.table.source_tables:
            if i_source_table.source_table_type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
                l_queue_source_table_list.append(i_source_table)
        if l_queue_source_table_list.__len__()>0: # формируем etl для каждой таблицы queue
            for i_queue_source_table in l_queue_source_table_list:
                l_source_table_corr_list=[]
                for i_source_table in self.table.source_tables:
                    if i_source_table.source_table_type!=const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
                        l_source_table_corr_list.append(i_source_table)
                    elif i_source_table==i_queue_source_table:
                        l_source_table_corr_list.append(i_source_table)
                l_etl_templates.append(self.get_etl_template(l_source_table_corr_list))
        else:
            l_etl_templates.append(self.get_etl_template(self.table.source_tables))
        return l_etl_templates


    def get_etl_template(self, p_source_tables: list):
        """
        Заменяет значения переменных в шаблоне etl

        :param p_source_tables: Список таблиц источников
        """
        l_etl_template_variables_dict={} # финальный список со словарями переменных и подсталенных значений
        # проставляем значения переменных у таблицы приемника
        l_target_table_dict=self.table.table_etl_template_variables.get(const('C_TABLE').constant_value,None) # переменные для таблицы приемника лежат в субсловаре с ключем table
        l_target_table_dict_keys=list(l_target_table_dict.keys()) # список ключей словаря
        for i_target_table_dict_key in l_target_table_dict_keys: # проходимся по всем переменным
            for i_attribute in self.table.table_attrs: # проходимся по всем атрибутам таблицы приемника
                if i_attribute.attribute_type==i_target_table_dict_key: # если тип атрибута таблицы приемника совпадает с ключем переменной
                    l_etl_template_variables_dict.update(
                        {
                            l_target_table_dict.get(i_target_table_dict_key,None):i_attribute.attribute_id # просталяем id атрибутов
                        }
                    )
        # отдельно проставляем id таблицы приемника
        l_etl_template_variables_dict.update(
            {
                l_target_table_dict.get(self.table.table_type,None):self.table.table_id
            }
        )
        # если queue таблица, то отдельная обработка
        if self.table.table_type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
            # проставляем перечисление атрибутов через запятую
            l_etl_template_variables_dict.update(
                {
                    self.table.table_etl_template_variables.get(const("C_LIST_OF_ATTRIBUTES").constant_value,None):self.table.attribute_id_sql()
                }
            )
            # проставляем перечисление атрибутов с cast
            l_etl_template_variables_dict.update(
                {
                    self.table.table_etl_template_variables.get(const("C_VALUE_DATA_SOURCE").constant_value,None):self.table.cast_attribute_sql()
                }
            )
        # проставляем значения переменных у таблиц источников
        l_source_table_etl_template_types=list(self.table.table_etl_template_variables.keys()) # список типов всех таблиц из словаря с переменными
        for i_source_table_template_type in l_source_table_etl_template_types: # проходимся по всем типам таблиц источников
            l_concat_nk_list=[] # список id атрибутов натуральных ключей для конкатенации
            l_concat_link_nk=[] # список id атрибутов натуральных ключей связанной сущности для конкатенации
            l_queue_table_id=None # id таблицы queue
            l_source_id=None # id источника queue таблицы
            for i_source_table in p_source_tables: # проходимся по всем таблицам источникам
                if i_source_table.source_table_type==i_source_table_template_type: # если тип таблицы источника совпадает с типом из словаря с переменными
                    l_source_table_template_variables=self.table.table_etl_template_variables.get(i_source_table_template_type,None) # получаем субсловарь переменных для атрибутов таблицы источника
                    l_source_table_etl_template_keys=list(l_source_table_template_variables.keys()) # получаем список типов атрибутов таблицы источника из субсловаря
                    for i_source_table_etl_template_keys in l_source_table_etl_template_keys: # проходимся по всем типам атрибутов таблиц источников из субсловаря
                        for i_source_attribute in i_source_table.source_table_attr: # проходимся по всем атрибутам таблицы источника
                            if i_source_attribute.source_attribute_type==i_source_table_etl_template_keys: # если тип атрибута таблицы источника совпадает с типом атрибута из субсловаря
                                # подставляем значение атрибута
                                l_etl_template_variables_dict.update(
                                    {
                                        l_source_table_template_variables.get(i_source_table_etl_template_keys,None):
                                            i_source_attribute.attribute.attribute_id # подставляем id атрибута
                                    }
                                )
                                # формируем список id атрибутов, которые требуется конкатенировать для генерации суррогата
                                # натуральный ключ (nk)
                                if i_source_attribute.source_attribute_type==const('C_NK_TYPE_NAME').constant_value \
                                        and i_source_table.source_table_type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
                                    l_concat_nk_list.append(i_source_attribute.attribute.attribute_id)
                                # натуральный ключ связанной сущности (link_nk), используется в tie
                                if i_source_attribute.source_attribute_type==const('C_LINK_NK_TYPE_NAME').constant_value \
                                        and i_source_table.source_table_type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
                                    l_concat_link_nk.append(i_source_attribute.attribute.attribute_id)
                    # проставляем id таблицы
                    l_etl_template_variables_dict.update(
                        {
                            l_source_table_template_variables.get(i_source_table_template_type,None):
                                i_source_table.table.table_id
                        }
                    )
                    if i_source_table.source_table_type==const('C_QUEUE_TABLE_TYPE_NAME').constant_value:
                        l_queue_table_id=i_source_table.table.table_id # определение id queue таблицы (используется далее)
                        l_source_id=i_source_table.source_id # определение id источника queue таблицы (используется далее)

            # заменяем переменные конкатенации у nk
            if l_concat_nk_list.__len__()>0: # не запускаем, когда лист пуст
                l_etl_template_variables_dict.update(
                    {
                        const('C_CONCAT_NK_VAR').constant_value:self.get_concat_script(l_concat_nk_list)
                    }
                )
            # заменяем переменные конкатенации у link_nk, используется в tie
            if l_concat_link_nk.__len__()>0: # не запускаем, когда лист пуст
                l_etl_template_variables_dict.update(
                    {
                        const('C_CONCAT_LINK_NK_VAR').constant_value:self.get_concat_script(l_concat_link_nk)
                    }
                )
            # отдельно проставляем наименования временных таблиц
            if l_queue_table_id is not None: # наименование временной таблицы просталяется в соответствии с queue таблицей
                l_temp_table_dict=self.table.table_etl_template_variables.get(const('C_TEMP_TABLE').constant_value,None) # достаем субсловарь с переменными для временных таблиц
                l_temp_table_dict_keys=list(l_temp_table_dict.keys()) # достаем ключи субсловаря временных таблиц
                for i_temp_table_key in l_temp_table_dict_keys: # проходимся по всем временным таблицам
                    l_etl_template_variables_dict.update(
                        {
                            l_temp_table_dict.get(i_temp_table_key,None):self.get_temp_table_name(p_queue_table_id=l_queue_table_id,p_temp_table_num=i_temp_table_key)
                            # наименование временной таблицы формируется на основе id queue таблицы и порядкового номера временной таблицы
                        }
                    )
            # отдельно проставляем id источника queue таблицы
            if l_source_id is not None:
                l_etl_template_variables_dict.update(
                    {
                        const('C_SOURCE_VAR').constant_value:l_source_id
                    }
                )
        return l_etl_template_variables_dict

    def get_etl_script(self, p_template):
        """
        Сгенерированный ETL таблицы

        :param p_template: шаблон с замененными переменными
        """
        l_table_etl_template=Driver.Template(
            p_table_type=self.table.table_type,
            p_template_type=const('C_ETL').constant_value
        )
        # заменяем в шаблоне переменные на значения
        l_table_etl = l_table_etl_template.template_file.replace_in_body(p_template)
        return l_table_etl

    def get_all_etl_script(self, p_template_list):
        """
        Формирует etl-скрипты для каждой шаблона из списка и отдает их списком
        :param p_template_list: список etl шаблонов
        """
        l_etl_script_list=[] # список etl-скриптов для таблицы
        for i_template in p_template_list:
            l_etl_script_list.append(self.get_etl_script(i_template))
        return l_etl_script_list

    def get_table_etl(self):
        """
        Формирует лист с etl-скриптами для таблицы
        """
        l_template_list=self.get_all_etl_templates() # формируем словари с переменными и их значениями
        return self.get_all_etl_script(l_template_list) # отдаем etl-скрипты

    def get_data_extract_script(self):
        """
        Формирует sql-запрос к источнику (только для queue таблиц)
        """
        if self.table.table_type!=const("C_QUEUE_TABLE_TYPE_NAME").constant_value:
            sys.exit("Таблица "+self.table.table_id+" не queue таблица") #TODO: реализовать вывод ошибок, как сделал Рустем
        l_data_extract_sql="select "+"\n"+self.table.attribute_name_sql(p_attribute_type=[const("C_QUEUE_ATTR_TYPE_NAME").constant_value])
        l_data_extract_sql=l_data_extract_sql+"\n"+"from "+'"'+str(self.table.source_schema)+'"'+"."+'"'+str(self.table.source_table_name)+'"'
        return l_data_extract_sql