import Metadata
import Support
import Source
import Driver
import ETL
import sys
import SQLScript
import pyodbc
import datetime
import copy

# Dummy transaction uuid 
C_DUMMY_TRAN = {"id": "67b2aba4-c34f-4999-a3eb-62ebb77a3776"}

# класс по загрузке данных в ХД
class DataLoad:
    #TODO: Устаревший класс
    # прогрузка данных в таблицу очереди
    @staticmethod
    def to_queue_table(queue_table, tran):
        # достаем скрипт отбора данных с источника
        select_from_source_sql=Metadata.Metadata.select_meta("queue_table_etl",None,0,{"queue_table":queue_table,"type":"select source data"})[0].get("sql",None)
        queue_table_attr=Metadata.Metadata.select_meta("queue_table",queue_table,0)[0]
        # достаем параметры источника
        source_table_attr=Metadata.Metadata.select_meta("source_table",queue_table_attr.get("source_table",None),0)[0]
        src_prm=Metadata.Metadata.select_meta("source",source_table_attr.get("source",None),0)[0]
        # достаем наименование инкремента
        incr_nm=source_table_attr.get("increment_datatype")
        # достаем тип инкремента
        incr_datatype=source_table_attr.get("increment_datatype")
        # достаем последнее значение инкремента
        incr_vl=Metadata.Metadata.select_meta("queue_table_last_etl_log",None,None,{"queue_table":queue_table})
        if len(incr_vl)>0 and incr_vl.get("increment_value",None) is not None:
            incr_vl=incr_vl.get("increment_value",None)
        else:
            incr_vl=Driver.Driver.increment_default_value(src_prm,incr_datatype)
        select_from_source_sql=select_from_source_sql.replace('@',"'"+str(incr_vl)+"'")
        # выполняем скрипт
        source_data_frame=Driver.Driver.sql_exec(src_prm,select_from_source_sql)
        # формируем значения для вставки
        insert_data_frame=[]
        for i in source_data_frame:
            row=[]
            for j in i:
                j=str(j).replace("'","''") # экранируем одинарную кавычку ' в строке
                row.append(j)
            insert_data_frame.append(row)
        insert_data_frame=str(insert_data_frame).replace('[','(').replace(']',')')
        insert_data_frame=insert_data_frame.replace(', "',",'").replace('",',"',").replace('")',"')").replace('("',"('") # при наличии одинарной кавычки в строке python оборачивает строку в двойные кавычки, вместо одинарных
        insert_data_frame=insert_data_frame[1:-1]
        #return insert_data_frame
        # формируем скрипт создания таблицы шины и вставки данных
        create_and_insert_sql=Metadata.Metadata.select_meta("queue_table_etl",None,0,{"queue_table":queue_table,"type":"bus insert"})[0].get("sql",None)
        create_and_insert_sql=create_and_insert_sql+insert_data_frame
        Driver.Driver.sql_exec(Driver.Driver.dwh_cnct_attr,create_and_insert_sql,1)
        # формируем скрипт вставки данных в таблицу очереди
        insert_queue_sql=Metadata.Metadata.select_meta("queue_table_etl",None,0,{"queue_table":queue_table,"type":"queue insert"})[0].get("sql",None)
        # заменяем параметры технических атрибутов
        # deleted_flg
        insert_queue_sql=insert_queue_sql.replace("@deleted_flg@","'0'") # пока не реализован функционал проставления признака удаления, всем ставим 0
        # status_id
        insert_queue_sql=insert_queue_sql.replace("@status_id@","'4f4c3fe8-d929-4587-8e1b-270d5432f8a4'") # пока не реализован функционал проверок данных, всем ставим дефолтное значение
        # etl_id
        etl_id=tran.get("id",None)
        insert_queue_sql=insert_queue_sql.replace("@etl_id@","'"+str(etl_id)+"'")
        Driver.Driver.sql_exec(Driver.Driver.dwh_cnct_attr,insert_queue_sql,1)
        # запись лога
        ETL.ETL.etl_log("queue_table_etl_log",queue_table,tran)
        # перезапись последнего лога
        ETL.ETL.last_etl_log("queue_table_last_etl_log",queue_table,tran)

    # загрузка данных в idmap
    @staticmethod
    def to_idmap(idmap_table, tran):
        # достаем скрипт отбора данных с источника
        idmap_sql=Metadata.Metadata.select_meta("idmap_etl",None,0,{"idmap":idmap_table,"type":"generate rk"})
        # запускаем скрипт
        # так как источников у rk может быть несколько - цикл
        for a in idmap_sql:
            Driver.Driver.sql_exec(Driver.Driver.dwh_cnct_attr,a.get("sql",None),1)
        # запись лога
        ETL.ETL.etl_log("idmap_etl_log",idmap_table,tran)
        # перезапись последнего лога
        ETL.ETL.last_etl_log("idmap_last_etl_log",idmap_table,tran)

    # загрузка данных в anchor
    @staticmethod
    def to_anchor(anchor_table, tran):
        # достаем скрипт отбора данных с источника
        anchor_sql=Metadata.Metadata.select_meta("anchor_table_etl",None,0,{"anchor_table":anchor_table,"type":"anchor insert"})[0].get("sql",None)
        # запускаем скрипт
        Driver.Driver.sql_exec(Driver.Driver.dwh_cnct_attr,anchor_sql,1)
        # запись лога
        ETL.ETL.etl_log("anchor_etl_log",anchor_table,tran)
        # перезапись последнего лога
        ETL.ETL.last_etl_log("anchor_last_etl_log",anchor_table,tran)

    # загрузка данных в attribute
    @staticmethod
    def to_attribute(attribute_table, tran):
        # достаем скрипт отбора данных с источника
        anchor_sql=Metadata.Metadata.select_meta("anchor_table_etl",None,0,{"anchor_table":attribute_table,"type":"attribute insert"})[0].get("sql",None)
        # запускаем скрипт
        Driver.Driver.sql_exec(Driver.Driver.dwh_cnct_attr,anchor_sql,1)
        # запись лога
        ETL.ETL.etl_log("anchor_etl_log",attribute_table,tran)
        # перезапись последнего лога
        ETL.ETL.last_etl_log("anchor_last_etl_log",attribute_table,tran)

    # загрузка данных в attribute
    @staticmethod
    def to_tie(tie_table, tran):
        # достаем скрипт отбора данных с источника
        tie_sql=Metadata.Metadata.select_meta("anchor_table_etl",None,0,{"anchor_table":tie_table,"type":"tie insert"})[0].get("sql",None)
        # запускаем скрипт
        Driver.Driver.sql_exec(Driver.Driver.dwh_cnct_attr,tie_sql,1)
        # запись лога
        ETL.ETL.etl_log("anchor_etl_log",tie_table,tran)
        # перезапись последнего лога
        ETL.ETL.last_etl_log("anchor_last_etl_log",tie_table,tran)

class DataLoader:
    """
        Класс, загружающий данные по выбранной сущности
        Diveev - Устарел - брал p_source_table_uuid из meta.source_table, что не корректно
        На всякий случай оставил
    """

    def __init__(self, p_source_table_uuid: str, p_transaction_uuid: str=C_DUMMY_TRAN):
        """
            Конструктор
            p_source_table_uuid - uuid таблицы-источника 
            p_transaction_uuid - uuid транзакции, если не указан, 
            то берется uuid транзакции-пустышки
        """
        # Params check 
        if not isinstance(p_source_table_uuid, str):
            raise TypeError("DataLoader - source_table_uuid должен быть строкой!")
        if p_source_table_uuid is None:
            raise TypeError("DataLoader - source_table_uuid должен быть заполнен!") 
        
        l_source_table_result = Metadata.Metadata.select_meta(
            object="source_table",
            id=p_source_table_uuid
        )
        if len(l_source_table_result) == 0:
            raise ValueError("DataLoader - указанное значение source_table_uuid не возвращает строку")
        elif len(l_source_table_result) > 1:
            raise ValueError("DataLoader - указанное значение source_table_uuid возвращает более 1 строки")
        else:
            self._source_table_uuid = p_source_table_uuid
            self._transaction_uuid = p_transaction_uuid 
            self._source_table_name = l_source_table_result[0].get("name")
            self._source_column_uuid_list = l_source_table_result[0].get("source_column")

        """
            Queue table - копия таблицы с источника + технические поля processed_dttm, update_dttm, etl_id 
            У 1 сущности может быть несколько queue_table так как атрибуты могут храниться в разных таблицах, 
            или одна сущность может объединять несколько таблиц. Приходит инкремент / полный срез.
            + есть status_id, row_id, etl_id. 
        """
        self._queue_table_uuid_list = []
        l_queue_table_result = Metadata.Metadata.select_meta(
            object="queue_table",
            object_attr={"source_table": p_source_table_uuid}
        )
        if len(l_queue_table_result) == 0:
            raise ValueError("DataLoader - не найден uuid queue-таблицы")
        for i_dict in l_queue_table_result:
            self._queue_table_uuid_list.append(i_dict.get("id"))

        """
            Idmap table - это связь суррогатного ключа с натуральным ключем + источник.
            Только 1 таблица на 1 сущность!
        """
        self._idmap_table_name = "idmap_" + self._source_table_name
        l_idmap_table_result = Metadata.Metadata.select_meta(
            object="idmap",
            object_attr={"name": self._idmap_table_name}
        )
        if len(l_idmap_table_result) == 0:
            raise ValueError("DataLoader - не найден uuid idmap-таблицы")
        elif len(l_idmap_table_result) > 1:
            raise ValueError("DataLoader - найдено более одного uuid idmap-таблицы")
        else:
            self._idmap_table_uuid = l_idmap_table_result[0].get("id")

        """
            Anchor table - это перечень суррогатных ключей. 
            Нужна тк в idmap нельзя добавить deleted_flg. 
            Только 1 таблица на 1 сущность!
        """
        self._anchor_table_name = self._source_table_name + "_anch"
        l_anchor_table_result = Metadata.Metadata.select_meta(
            object="anchor_table",
            object_attr={"name": self._anchor_table_name}
        )
        if len(l_anchor_table_result) == 0:
            raise ValueError("DataLoader - не найден uuid anchor-таблицы")
        elif len(l_anchor_table_result) > 1:
            raise ValueError("DataLoader - найдено более одного uuid anchor-таблицы")
        else:
            self._anchor_table_uuid = l_anchor_table_result[0].get("id")

        """
            Attribute_table - связь атрибута сущности с натуральным ключем.
            Количество соответствует количеству заявленных атрибутов
            update_dttm - когда изменился атрибут - 2 варианта 
            1) согласно инкрементальным полям, 2) дата прогрузки в stg
            procesed_dttm - фактическая отметка когда появились данные в таблице
        """
        self._attribute_table_uuid_list = []
        for i_uuid in self._source_column_uuid_list:
            l_source_column_result = Metadata.Metadata.select_meta(
                object="source_column",
                id=i_uuid
            )
            if len(l_source_column_result) == 0:
                raise ValueError("DataLoader - не найден uuid source_column")
            l_source_column_name = l_source_column_result[0].get("name")
            l_attribute_table_name = self._source_table_name + \
                "_" + l_source_column_name + "_attr"
            l_anchor_table_result = Metadata.Metadata.select_meta(
                object="anchor_table",
                object_attr={"name": l_attribute_table_name}
            )
            if len(l_anchor_table_result) == 0:
                raise ValueError("DataLoader - не найден uuid attribute-таблицы!")
            elif len(l_anchor_table_result) > 1:
                raise ValueError("DataLoader - найдено более одного uuid у attribute-таблицы!")
            else:
                l_attribute_table_uuid = l_anchor_table_result[0].get("id")
                self._attribute_table_uuid_list.append(l_attribute_table_uuid)

        # TODO - жду доработку Паши
        """
            Tie table - связь двух сущностей - суррогатные ключи из обоих таблиц 
            Обычно на источнике есть оба атрибута, наличие связи выделяется пользователем
        """
        # self._tie_table_uuid = None

    def run_job(self):
        """
            Запуск прогрузки данных во все необходимые таблицы указанной сущности
        """
        # Queue table 
        for i_queue_table_uuid in self._queue_table_uuid_list:
            DataLoad.to_queue_table(
                queue_table=i_queue_table_uuid, 
                tran=self._transaction_uuid
            )
        # Idmap table 
        DataLoad.to_idmap(
            idmap_table=self._idmap_table_uuid,
            tran=self._transaction_uuid
        )
        # Anchor table 
        DataLoad.to_anchor(
            anchor_table=self._anchor_table_uuid,
            tran=self._transaction_uuid
        )
        # Attribute table 
        for i_attribute_table_uuid in self._attribute_table_uuid_list:
            DataLoad.to_attribute(
                attribute_table=i_attribute_table_uuid,
                tran=self._transaction_uuid
            )

def load_all_queue_tables(p_tran: str=C_DUMMY_TRAN):
    """
        Runs dml from metadata for all queue tables 
        p_tran - transaction uuid
    """
    l_queue_table_result = Metadata.Metadata.select_meta(
        object="queue_table"
    )
    for i_dict in l_queue_table_result:
        l_queue_table_uuid = i_dict.get("id")
        DataLoad.to_queue_table(
            queue_table=l_queue_table_uuid,
            tran=p_tran
        )

def load_all_idmap_tables(p_tran: str=C_DUMMY_TRAN):
    """
        Runs dml from metadata for all idmap tables
        p_tran - transaction uuid
    """
    l_idmap_result = Metadata.Metadata.select_meta(
        object="idmap",
    ) 
    for i_dict in l_idmap_result:
        l_idmap_uuid = i_dict.get("id")
        DataLoad.to_idmap(
            idmap_table=l_idmap_uuid,
            tran=p_tran
        )

def load_all_anchor_tables(p_tran: str=C_DUMMY_TRAN):
    """
        Runs dml from metadata for all anchor tables 
        p_tran - transaction uuid
    """
    l_anchor_table_result = Metadata.Metadata.select_meta(
        object="anchor_table",
        object_attr={"type": "anchor"}
    )
    for i_dict in l_anchor_table_result:
        l_anchor_table_uuid = i_dict.get("id")
        DataLoad.to_anchor(
            anchor_table=l_anchor_table_uuid,
            tran=p_tran
        )

def load_all_attribute_tables(p_tran: str=C_DUMMY_TRAN):
    """
        Runs dml from metadata for all attribute tables 
        p_tran - transaction uuid
    """
    l_anchor_table_result = Metadata.Metadata.select_meta(
        object="anchor_table",
        object_attr={"type": "attribute"}
    )
    for i_dict in l_anchor_table_result:
        l_attribute_table_uuid = i_dict.get("id")
        DataLoad.to_attribute(
            attribute_table=l_attribute_table_uuid,
            tran=p_tran
        )

def load_all_tie_tables(p_tran: str=C_DUMMY_TRAN):
    """
        Runs dml from metadata for all tie tables 
        p_tran - transaction uuid 
    """
    l_anchor_table_result = Metadata.Metadata.select_meta(
        object="anchor_table",
        object_attr={"type": "tie"}
    )
    for i_dict in l_anchor_table_result:
        l_tie_table_uuid = i_dict.get("id")
        DataLoad.to_tie(
            tie_table=l_tie_table_uuid,
            tran=p_tran
        )

def load_all(p_tran: str=C_DUMMY_TRAN):
    """
        Runs dml for all data warehouse tables from metadata
        Queue -> Idmap -> Anchor -> Attribute -> Tie 
        p_tran - transaction uuid
    """
    print("start loading")
    load_all_queue_tables(p_tran=p_tran)
    print("queue tables loaded")
    load_all_idmap_tables(p_tran=p_tran)
    print("idmap tables loaded")
    load_all_anchor_tables(p_tran=p_tran)
    print("anchor tables loaded")
    load_all_attribute_tables(p_tran=p_tran)
    print("attribute tables loaded")
    load_all_tie_tables(p_tran=p_tran)
    print("tie tables loaded")
