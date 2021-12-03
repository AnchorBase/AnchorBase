# coding=utf-8
import psycopg2
import psycopg2.extensions
import sys
import copy
import datetime
import json
import Metadata
import Support
from SystemObjects import Constant as const

#TODO: проблема со вставкой русских символов

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

class Postgres:
    #TODO: Устаревший класс
    # подключается к Postgres и выполняет запросы
    @staticmethod
    def sql_exec(cnct_attr, sql, noresult=None):
        query_output=""
        server=cnct_attr.get("server",None)
        database=cnct_attr.get("database",None)
        user=cnct_attr.get("user",None)
        password=cnct_attr.get("password",None)
        port=5433
        try:
            cnct = psycopg2.connect(dbname=database, user=user,
                                    password=password, host=server, port=port)
            cnct.autocommit = False
            crsr = cnct.cursor()
            crsr.execute(sql)
            if noresult == 0 or noresult is None:
                query_output = crsr.fetchall()
            else:
                query_output=""
            cnct.commit()
        except psycopg2.Error as e:
            error = Support.Support.error_output("Postgres","sql_exec",e)
            sys.exit(error)
        else: return query_output

    @staticmethod
    def create_table_ddl(schema, table, attr):
        #attr: {
        #           "column_id":"id атрибута"
        #           ,"datatype":"тип данных"
        #           ,"length":"Размер десятичного числа/строки"
        #           ,"scale":"Количество знаков после запятой"}]
        # source - указатель, что типы данных из сторонней СУБД (не PostgreSQL)
        attr_len = len(attr)
        script = "create table "+schema+"."+'"'+table+'"'+" ("
        attr_script = ""
        i=0
        while i<attr_len:
            datatype_length = attr[i].get("length",None)
            scale = ""
            datatype = attr[i].get("datatype",None)
            comma = ","
            if attr[i].get("scale",None) is not None and attr[i].get("scale",None)!="none":
                scale = ","+str(attr[i].get("scale",None))
            if datatype_length is not None and datatype_length!="none":
                datatype_length = "("+str(datatype_length)+scale+")"
            else:
                datatype_length = ""
            if i == attr_len - 1:
                comma=""
            attr_script = attr_script + '"'+ str(attr[i].get("id",None))+'"'+" "+datatype+datatype_length+comma
            i=i+1
        script = script + attr_script + ");"
        return script

    @staticmethod
    def drop_table_sql(schema, table):
        table=table.replace('"','')
        script = "drop table if exists "+schema+"."+'"'+table+'"'+";"
        return script

    @staticmethod
    def create_view_sql(schema, view, source_schema, source_table, attr, vers=None):
        # создает view
        # attr {"column_id":"1","column_name":"x"} -> select 1 as x
        # vers - добавить версионное поле на основе update_dttm - lead(update_dttm)...
        # vers - {
        #          'pk':'наименование ключа таблицы (для partition by)' -- может быть листом!!
        #        ,'dt':'наименование атрибута, на основе которого будет строиться версионность'
        #        }
        script = "drop view if exists "+schema+"."+'"'+view+'"'+"; create view "+schema+"."+'"'+view+'"'+" as select "
        attr_len = len(attr)
        i = 0
        attr_script = ""
        dt_from = ""
        dt_to = ""
        if vers is not None:
            if vers.get("dt",None) is None or vers.get("pk",None) is None:
                error = Support.Support.error_output("SQLScript","create_view_sql","There is no primary key or date attribute mentioned")
                sys.exit(error)

            pk=""
            for j in vers.get("pk",None):
                pk=pk+'"'+str(j)+'"'+","
            pk=pk.rstrip(",")
            dt_from = " ,coalesce("+'"'+vers["dt"]+'"'+",cast('1900-01-01' as timestamp)) as dt_from "
            dt_to = " ,coalesce(lead("+'"'+vers["dt"]+'"'+") over (partition by "+pk+" order by "+'"'+vers["dt"]+'"'+" asc),cast('5999-12-31' as timestamp)) as dt_to "
        while i<attr_len:
            if vers is not None:
                attr_name=str(attr[i]["name"])
            else:
                attr_name=str(attr[i]["id"])
            comma=""
            if i!=attr_len-1:
                comma=","
            attr_script = attr_script+'"'+attr_name+'"'+" as "+'"'+attr[i]["name"]+'"'+comma
            i=i+1
        script = script+attr_script+dt_from+dt_to+" from "+source_schema+"."+'"'+str(source_table)+'"'+";"
        return script

    # генерация скрипта для вью на актуальный срез
    # работает только для таблиц типа attribute
    @staticmethod
    def create_lv_view_sql(schema, view, lv_view, mxdttm_table, view_rk):
        # view - наименование обычной (не версионной) view
        # lv_view - наименование вью на актуальный срез
        # mxdttm_table - наименование таблицы, содержащей максимальную дату обновления
        # view_rk - наименование rk сущности
        script = "drop view if exists "+schema+"."+'"'+lv_view+'"'+"; create view "+schema+"."+'"'+lv_view+'"'+" as " \
                 "select vw.* from "+schema+"."+'"'+view+'"'+" as vw " \
                 "inner join "+schema+"."+'"'+mxdttm_table+'"'+" as mx " \
                 "on vw."+'"'+view_rk+'"'+"=mx.rk " \
                 "and vw.update_dttm=mx.max_update_dttm;"
        return script

    # скрипт, определяющий макс update_dttm
    @staticmethod
    def max_update_dttm_sql(table):
        # table - наименование view в якорной модели
        scrpt="select coalesce(max(update_dttm),cast('1900-01-01' as timestamp)) from anch."+'"'+str(table)+'"'+";"
        return scrpt

    # скрипт, создаюший таблицу шину и вставляющий в нее данные с источника
    @staticmethod
    def insert_data_into_bus(table, column):
        clmn_nm_scrpt=""
        clmn_script=""
        for a in column:
            clmn_nm_scrpt=clmn_nm_scrpt+'"'+str(a.get("name",None))+'"'+","
            length=""
            if a.get("length",None) is not None:
                    if a.get("scale",None) is not None:
                        length="("+str(a.get("length",None))+","+str(a.get("scale",None))+")"
                    else:
                        length="("+str(a.get("length",None))+")"
            clmn_script=clmn_script+'"'+str(a.get("name",None))+'" '+str(a.get("datatype",None))+length+","
        clmn_nm_scrpt=clmn_nm_scrpt[:-1]
        clmn_script=clmn_script[:-1]
        scrpt="drop table if exists stg."+'"'+str(table)+'"'+"; " \
              "create table stg."+'"'+str(table)+'"'+" ("+clmn_script+");" \
              "insert into stg."+'"'+str(table)+'"'+" ("+clmn_nm_scrpt+") values "
        return scrpt

    # скрипт, вставляющий данные с помощью select
    @staticmethod
    def insert_data_into_queue(table, column, source_table):
        clmn_nm_scrpt=""
        for a in column:
            clmn_nm_scrpt=clmn_nm_scrpt+'"'+str(a.get("name",None))+'"'+","
        # технические атрибуты
        tech_clmn_nm_scrpt=" deleted_flg, update_dttm, processed_dttm, status_id, etl_id, row_id"
        tech_clmn_scrpt=" cast(@deleted_flg@ as integer) as deleted_flg,"\
                        " cast(@update_dttm@ as timestamp) as update_dttm,"\
                        " cast(current_timestamp as timestamp) as processed_dttm,"\
                        " cast(@status_id@ as uuid) as status_id,"\
                        " cast(@etl_id@ as uuid) as etl_id,"\
                        " cast(uuid_generate_v4() as uuid) as row_id"
        scrpt="insert into stg."+'"'+str(table)+'"'+" ("+clmn_nm_scrpt+tech_clmn_nm_scrpt+") select "+clmn_nm_scrpt+tech_clmn_scrpt+" from stg."+'"'+str(source_table)+'"'+";"
        # удаляем таблицу шины
        drop_scrpt=Postgres.drop_table_sql("stg",source_table)
        scrpt=scrpt+drop_scrpt
        return scrpt

    # current_timestamp
    @staticmethod
    def current_timestamp_sql():
        return "current_timestamp"

    # generate guid
    @staticmethod
    def generate_uuid_sql():
        return "uuid_generate_v4()"

    # генерация rk в idmap
    @staticmethod
    def insert_data_into_idmap(idmap, nkey, queue_table, source_id):
        # idmap - наименование idmap
        # nkey - лист наименований натуральных ключей
        # queue_table - наименование таблицы queue
        # формируем конкатенацию натуральных ключей
        nkey_cnct=""
        for a in nkey:
            nkey_cnct=nkey_cnct+"cast("+'"'+str(a)+'"'+" as varchar(1000))||''@@''||"
        nkey_cnct="cast("+nkey_cnct+"cast(''"+str(source_id)+"'' as varchar(1000))"+" as varchar(1000))"
        idmap_rk='"'+str(idmap).replace("idmap_","")+"_rk"+'"'
        idmap_nk='"'+str(idmap).replace("idmap_","")+"_nk"+'"'
        idmap_bus='"'+str(idmap)+'_bus"'
        drop_scrpt="drop table if exists idmap."+idmap_bus+";" # удалем idmap шину на всякий случай
        # создаем временную таблицу с конкатенированными натуральным ключами для генерации
        create_scrpt="create table idmap."+idmap_bus+" as " \
                     "select "+nkey_cnct+" as "+idmap_nk+" from stg."+'"'+str(queue_table)+'"'+";"
        insert_scprt="insert into idmap."+'"'+str(idmap)+'"' \
              " ("+idmap_rk+","+idmap_nk+")" \
              " select uuid_generate_v4(), q."+idmap_nk+" " \
              " from idmap."+idmap_bus+" as q" \
              " left join idmap."+'"'+str(idmap)+'" as idmp ' \
              " on q."+idmap_nk+"=idmp."+idmap_nk+" " \
              " where idmp."+idmap_rk+" is null;"
        scrpt=drop_scrpt+create_scrpt+insert_scprt+drop_scrpt
        return scrpt

    # загрузка данных в anchor
    @staticmethod
    def insert_data_into_anchor(anchor_table, idmap_table):
        # anchor_table - наименование anchor таблицы
        # idmap_table - наименование idmap таблицы
        # source_id - uuid источника
        anchor_rk='"'+str(idmap_table).replace("idmap_","")+"_rk"+'"' # суррогат сущности
        anchor_nk=anchor_rk.replace("_rk","_nk")
        anchor_name='"'+str(anchor_table)+'"'
        idmap_name='"'+str(idmap_table)+'"'
        source_id_scrpt="cast(reverse(substring(reverse("+anchor_nk+"),1,strpos(reverse("+anchor_nk+"),''@@'')-1)) as uuid)" # извлекаем source_id
        cur_tmstmp=Postgres.current_timestamp_sql()
        insert_scrpt="insert into anch."+anchor_name+" " \
                     "("+anchor_rk+", source_system_id, processed_dttm) " \
                     "select idmp."+anchor_rk+","+source_id_scrpt+","+cur_tmstmp+" " \
                     " from idmap."+idmap_name+" as idmp " \
                     " left join anch."+anchor_name+" as anch " \
                     " on idmp."+anchor_rk+"=anch."+anchor_rk+" " \
                     " where anch."+anchor_rk+" is null;"
        return insert_scrpt

    # загрузка данных в attribute
    @staticmethod
    def insert_data_into_attribute(
            attribute_table:str,
            attribute_lv_view:str,
            attribute_column:str,
            anchor_column:str,
            idmap_table:str,
            idmap_key:str,
            mxdttm_table:str,
            queue_obj:list
    ):
        """
        Возвращает SQL-скрипт загрузки данных в таблицу типа attribute
        input:
            attribute_table - наименование таблицы типа attribute
            attribute_lv_view - наименование view на актуальный срез таблицы типа attribute
            attribute_column - наименование атрибута в таблице типа attribute
            anchor_column - наименование rk в таблице типа attribute
            idmap_table - наименование idmap сущности
            idmal_key - наименование атрибута nk в idmap
            mxdttm_table - наименование таблицы с максимальными update_dttm для атрибута
            queue_obj - лист с source_id, queue_table, queue_column и queue_key (список из наименований натуральный ключей) [{"source_id":"","queue_table":"","queue_column":"","queue_key":[]}]
        output:
            SQL-скрипт
    """
        rnum_table='"'+str(attribute_table)+'_rnum"'
        vers_table='"'+str(attribute_table)+'_vers"'
        attribute_lv_view='"'+str(attribute_lv_view)+'"'
        anchor_column='"'+str(anchor_column)+'"'
        attribute_column='"'+str(attribute_column)+'"'
        processed_dttm=Postgres.current_timestamp_sql()
        idmap_table='"'+str(idmap_table)+'"'
        idmap_key='"'+str(idmap_key)+'"'
        # создаем промежуточную таблицу с нумерацией уникальных значений атрибута в соответствии с update_dttm
        drop_rnum_table=Postgres.drop_table_sql("anch",rnum_table)
        # create_rnum_table="create table anch."+'"'+rnum_table+'"'+" as "
        create_rnum_table="create table anch."+rnum_table+" as "
        rnum="row_number() over (partition by idmap."+anchor_column+" order by qe.update_dttm) as rnum "
        select_rnum_table=""
        for a in queue_obj:
            queue_column='"'+str(a.get("queue_column",None))+'"'
            queue_table='"'+str(a.get("queue_table",None))+'"'
            source_id=str(a.get("source_id",None))
            queue_key=""
            for b in a.get("queue_key",None): #TODO копия скрипта из insert_data_into_idmap - по-хорошему вынести в отдельный метод
                queue_key=queue_key+"cast("+'qe."'+str(b)+'"'+" as varchar(1000))||''@@''||"
            queue_key="cast("+queue_key+"cast(''"+str(source_id)+"'' as varchar(1000))"+" as varchar(1000))"
            select_rnum_table=select_rnum_table+" select idmap."+anchor_column+", qe."+queue_column+" as "+attribute_column+", qe.update_dttm, "+rnum+" " \
                            "from stg."+queue_table+" as qe inner join idmap."+idmap_table+" as idmap " \
                            "on 1=1 and "+queue_key+"=idmap."+idmap_key
            if queue_obj.index(a)==len(queue_obj)-1:
                select_rnum_table=select_rnum_table+";"
            else:
                select_rnum_table=select_rnum_table+" union all "
        drop_vers_table=Postgres.drop_table_sql("anch",vers_table)
        create_vers_table="create table anch."+vers_table+" as "
        select_vers_table="select crow."+anchor_column+", crow."+attribute_column+", crow.update_dttm, crow.rnum " \
                          "from anch."+rnum_table+" as crow left join anch."+rnum_table+" as prow " \
                          "on 1=1 and crow."+anchor_column+"=prow."+anchor_column+" " \
                          "and crow.rnum=prow.rnum+1 and crow."+attribute_column+"=prow."+attribute_column+" " \
                          "where 1=1 and prow."+anchor_column+" is null;"
        insert_attr_table="insert into anch."+attribute_table+" ("+anchor_column+", "+attribute_column+", update_dttm, processed_dttm) " \
                          "select vers."+anchor_column+", vers."+attribute_column+",vers.update_dttm, "+processed_dttm+" " \
                          "from anch."+vers_table+" as vers left join anch."+attribute_lv_view+" as lv " \
                          "on 1=1 and vers."+anchor_column+"=lv."+anchor_column+" and vers."+attribute_column+"=lv."+attribute_column+" " \
                          "and vers.rnum=1 " \
                          "where 1=1 and lv."+anchor_column+" is null;"
        update_mxdttm_table=Postgres.update_maxdttm_table_sql(mxdttm_table,attribute_table,anchor_column)
        script=drop_rnum_table+create_rnum_table+select_rnum_table+drop_vers_table+create_vers_table+select_vers_table+drop_rnum_table+insert_attr_table+drop_vers_table+update_mxdttm_table
        return script
    # обнволяет техническую таблицу, храняющую суррогат и максимальное значение update_dttm атрибута
    @staticmethod
    def update_maxdttm_table_sql(maxdttm_table, attribute_table,anchor_column):
        # maxdttm_tabl - наименование технической таблицы
        # attribute_table - наименование таблицы типа аттрибут
        # anchor_column - наименование поля rk
        # происходит полное обновление таблицы
        maxdttm_table='"'+str(maxdttm_table)+'"'
        delete_sql="delete from anch."+maxdttm_table+";" #удаляем все данные
        insert_sql="insert into anch."+maxdttm_table+" (rk,max_update_dttm) " \
                   "select attr."+anchor_column+" as rk, max(attr.update_dttm) as max_update_dttm " \
                   "from anch."+attribute_table+" as attr " \
                   "group by attr."+anchor_column+";"
        scrpt=delete_sql+insert_sql
        return scrpt

    @staticmethod
    def insert_data_into_tie(
            tie_table:str,
            tie_rk1:str,
            tie_rk2:str,
            idmap1_table:str,
            idmap2_table:str,
            idmap_key1:str,
            idmap_key2:str,
            queue_obj:list
    ):
        """
        Возвращает SQL-скрипт загрузки данных в таблицу типа tie
        input:
            tie_table - наименование таблицы типа tie
            tie_rk1 - наименование первого ключа tie,
            tie_rk2 - наименование второго ключа tie,
            idmap1_table - наименование idmap для первого ключа,
            idmap2_table - наименование idmap для второго ключа,
            idmap_key1 - наименование натурального ключа idmap для первого ключа,
            idmap_key2 - наименование натурального ключа idmap для второго ключа,
            queue_obj - лист с параметрами источника для ключей tie: source_id, queue_table, queue_key1 и queue_key2 (списки из наименований натуральных ключей) [{"source_id":"","queue_table":"","queue_key1":[], queue_key2":[]}]
        output:
            SQL-скрипт
        """
        union_table='"'+str(tie_table)+'_union"'
        tie_table='"'+str(tie_table)+'"'
        tie_rk1='"'+str(tie_rk1)+'"'
        tie_rk2='"'+str(tie_rk2)+'"'
        drop_union_table=Postgres.drop_table_sql("anch",union_table)
        create_union_table="create table anch."+union_table+" as "
        idmap_key1='"'+str(idmap_key1)+'"'
        idmap_key2='"'+str(idmap_key2)+'"'
        idmap1_table='"'+str(idmap1_table)+'"'
        idmap2_table='"'+str(idmap2_table)+'"'
        processed_dttm=Postgres.current_timestamp_sql()
        select_union_table=""
        for a in queue_obj:
            queue_table='"'+str(a.get("queue_table",None))+'"'
            source_id=str(a.get("source_id",None))
            queue_key1=""
            queue_key2=""
            for b in a.get("queue_key1",None): #TODO копия скрипта из insert_data_into_idmap - по-хорошему вынести в отдельный метод
                    queue_key1=queue_key1+"cast("+'qe."'+str(b)+'"'+" as varchar(1000))||''@@''||"
            for b in a.get("queue_key2",None):
                queue_key2=queue_key2+"cast("+'qe."'+str(b)+'"'+" as varchar(1000))||''@@''||"
            queue_key1="cast("+queue_key1+"cast(''"+str(source_id)+"'' as varchar(1000))"+" as varchar(1000))"
            queue_key2="cast("+queue_key2+"cast(''"+str(source_id)+"'' as varchar(1000))"+" as varchar(1000))"
            select_union_table=select_union_table+" select "+queue_key1+" as "+idmap_key1+", "+queue_key2+" as "+idmap_key2+", min(qe.update_dttm) as update_dttm " \
                               "from stg."+queue_table+" as qe " \
                               "group by "+queue_key1+", "+queue_key2
            if queue_obj.index(a)==len(queue_obj)-1:
                select_union_table=select_union_table+";"
            else:
                select_union_table=select_union_table+" union all "
            insert_into_tie="insert into anch."+tie_table+" ("+tie_rk1+","+tie_rk2+",update_dttm,processed_dttm) "\
                            "select idmap1."+tie_rk1+", idmap2."+tie_rk2+",un.update_dttm,"+processed_dttm+" " \
                            "from anch."+union_table+" as un " \
                            "left join idmap."+idmap1_table+" as idmap1 " \
                            "on 1=1 and un."+idmap_key1+"=idmap1."+idmap_key1+" " \
                            "left join idmap."+idmap2_table+" as idmap2 " \
                            "on 1=1 and un."+idmap_key2+"=idmap2."+idmap_key2+" " \
                            "left join anch."+tie_table+" as tie " \
                            "on 1=1 and idmap1."+tie_rk1+"=tie."+tie_rk1+" " \
                            "and idmap2."+tie_rk2+"=tie."+tie_rk2+" " \
                            "where 1=1 and tie."+tie_rk1+" is null and idmap1."+tie_rk1+" is not null and idmap2."+tie_rk2+" is not null;"
            script=drop_union_table+create_union_table+select_union_table+insert_into_tie+drop_union_table
            return script


def sql_exec(
        p_database: str,
        p_server: str,
        p_user: str,
        p_password: str,
        p_port: int,
        p_sql: str,
        p_result: int =1
):
    """
    Выполняет запросы в PostgreSQL
    :param p_database: база данных
    :param p_server: сервер
    :param p_user: пользователь
    :param p_password: пароль
    :param p_port: порт
    :param p_sql: SQL-запрос
    :param p_result: Признак наличия результата запроса (по умолчанию 1)
    """
    l_query_output=None
    l_error=None
    try: # проверяем подключение
        cnct = psycopg2.connect(
            dbname=p_database,
            user=p_user,
            password=p_password,
            host=p_server,
            port=p_port
        )
    except psycopg2.OperationalError as e:
        # sys.exit(e) #TODO: реализовать вывод ошибок, как сделал Рустем
        l_error=e
    cnct.autocommit = False
    crsr = cnct.cursor()
    try:
        crsr.execute(p_sql)
        if p_result==1: # если нужен результат запроса
            l_query_output = crsr.fetchall()
        else:
            l_query_output = 1
        cnct.commit() # комит транзакции
    except psycopg2.Error as e:
        cnct.rollback() # при возникновении ошибки - откат транзакции
        # sys.exit(e) #TODO: реализовать вывод ошибок, как сделал Рустем
        l_error=e
    finally:
        crsr.close()
        cnct.close()
    return l_query_output, l_error

C_CURRENT_TIMESTAMP_SQL="CURRENT_TIMESTAMP"

def get_source_table_etl(
        p_source_table_id: str,
        p_source_attribute: str,
        p_source_attribute_value: str
):
    """
    Генерирует ETL-скрипт для queue таблицы

    :param p_source_table_id: id таблицы
    :param p_source_attribute: атрибуты таблицы (список должен быть отсортирован в соответствии с наименованием атрибута)
    :param p_source_attribute_value: значения атрибутов
    """
    l_etl="INSERT INTO "+'"'+const('C_STG_SCHEMA').constant_value+'"'+"."+'"'+str(p_source_table_id)+'"'+"\n\t"\
          "("+p_source_attribute+")\n\t"\
          "VALUES\n\t"+p_source_attribute_value+";\n"
    return l_etl


def get_idmap_etl(
      p_idmap_id: str,
      p_source_table_id: str,
      p_attribute_nk: str,
      p_source_id: str,
      p_idmap_nk_id: str,
      p_idmap_rk_id: str,
      p_etl_id: str,
      p_etl_value: str
):
    """
    Генерирует ETL для Idmap

    :param p_idmap_id: id idmap
    :param p_source_table_id: id таблицы источника
    :param p_attribute_nk: скрипт конкатенации натуральных ключей из таблицы источника
    :param p_source_id: source_system_id источника
    :param p_idmap_nk_id: id атрибута натурального ключа
    :param p_idmap_rk_id: id атрибута суррогатного ключа
    :param p_etl_id: id атрибута etl_id
    :param p_etl_value: значение для атрибута etl_id
    """
    l_etl="DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+"_1"+'"'+";\n"\
          "CREATE TABLE "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+"_1"+'"'+" AS (\n"\
          "SELECT\n\t"\
          "DISTINCT\n\t"\
          "CAST(\n\t\t"+p_attribute_nk+"\n\t\t||'@@'||\n\t\t"+"CAST('"+str(p_source_id)+"' AS VARCHAR(1000))\n\t"\
          "AS VARCHAR (1000)) AS idmap_nk\n\t"\
          "FROM "+'"'+const('C_STG_SCHEMA').constant_value+'"'+"."+'"'+str(p_source_table_id)+'"\n'+");\n"\
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+"_2"+'"'+";\n"\
          "CREATE TABLE "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+"_2"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          "nk.idmap_nk\n\t"\
          ",mx_rk.max_rk + ROW_NUMBER() OVER (ORDER BY 1) AS idmap_rk\n\t"\
          "FROM "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+"_1"+'"'+" as nk\n\t"\
          "LEFT JOIN "+'"'+const('C_IDMAP_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+'"'+" as idmp\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND nk.idmap_nk=idmp."+'"'+str(p_idmap_nk_id)+'"'+"\n\t" \
          "CROSS JOIN (\n\t\tSELECT \n\t\tMAX("+'"'+str(p_idmap_rk_id)+'"'+") as max_rk \n\t\tFROM "\
          +'"'+const('C_IDMAP_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+'"'+"\n\t) as mx_rk\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND idmp."+'"'+str(p_idmap_rk_id)+'"'+" IS NULL\n);\n"\
          "INSERT INTO idmap."+'"'+str(p_idmap_id)+'"'+"\n"\
          "(\n\t"+'"'+str(p_idmap_rk_id)+'"\n\t'+","+'"'+str(p_idmap_nk_id)+'"\n\t'+","+'"'+str(p_etl_id)+'"\n'+")\n\t"\
          "SELECT\n\t"\
          "idmap_rk,\n\t"\
          "idmap_nk,\n\tCAST('"+str(p_etl_value)+"' AS BIGINT)\n\t"\
          "FROM "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+"_2"+'"'+"\n;\n"\
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+"_1"+'"'+";\n"\
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+"_2"+'"'+";"
    return l_etl

def get_anchor_etl(
        p_anchor_id: str,
        p_anchor_rk_id: str,
        p_anchor_source_id: str,
        p_anchor_etl_id: str,
        p_idmap_id: str,
        p_idmap_rk_id: str,
        p_idmap_nk_id: str,
        p_etl_value: str
):
    """
    Герерирует etl для anchor таблицы

    :param p_anchor_id: id якорной таблицы
    :param p_anchor_rk_id: id rk атрибута якорной таблицы
    :param p_anchor_source_id: id атрибута source_system_id
    :param p_anchor_etl_id: id атрибута etl_id
    :param p_idmap_id: id idmap
    :param p_idmap_rk_id: id атрибута rk idmap
    :param p_idmap_nk_id: id атрибута nk idmap
    :param p_etl_value: etl процесс
    """
    l_etl="INSERT INTO "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_anchor_id)+'"'+"\n(\n\t"\
          +'"'+str(p_anchor_rk_id)+'"'+",\n\t"+'"'+str(p_anchor_source_id)+'"'+",\n\t"+'"'+str(p_anchor_etl_id)+'"'+"\n)\n\t"\
          "SELECT\n\t" \
          +'"'+str(p_idmap_rk_id)+'"'+"\n\t"\
         ",CAST(REVERSE(SUBSTR(REVERSE("+'"'+str(p_idmap_nk_id)+'"'+"),1,POSITION('@@' IN REVERSE("+'"'+str(p_idmap_nk_id)+'"'+"))-1)) AS INT)\n\t"\
         ",CAST('"+str(p_etl_value)+"' AS BIGINT)\n\t"\
         "FROM "+'"'+const('C_IDMAP_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+'"'+"\n\t"\
         "CROSS JOIN (\n\t\t"\
         "SELECT MAX("+'"'+str(p_anchor_rk_id)+'"'+") AS max_rk \n\t\t"\
         "FROM "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_anchor_id)+'"'+"\n\t) AS mx_rk\n\t"\
         "WHERE 1=1\n\t\t"\
         "AND " +'"'+str(p_idmap_rk_id)+'"'+">mx_rk.max_rk;"
    return l_etl

def get_attribute_etl(
        p_attribute_id: str,
        p_anchor_rk_id: str,
        p_attribute_column_id: str,
        p_from_dttm_id: str,
        p_to_dttm_id: str,
        p_etl_id: str,
        p_idmap_id: str,
        p_idmap_rk_id: str,
        p_idmap_nk_id: str,
        p_attribute_concat_nk: str,
        p_stg_table_id: str,
        p_stg_attribute_id: str,
        p_update_timestamp_id: str,
        p_source_id: str,
        p_etl_value: str,
        p_data_type: str
):
    """
    Генерирует etl для таблицы attribute

    :param p_attribute_id: id таблицы attribute
    :param p_anchor_rk_id: id атрибута rk таблицы attribute
    :param p_attribute_column_id: id атрибута value таблицы attribute
    :param p_from_dttm_id: id атрибута from таблицы attribute
    :param p_to_dttm_id: id атрибута to таблицы attribute
    :param p_etl_id: id атрибута etl таблицы attribute
    :param p_idmap_id: id таблицы idmap
    :param p_idmap_rk_id: id атрибута rk таблицы idmap
    :param p_idmap_nk_id: id атрибута nk таблицы idmap
    :param p_attribute_concat_nk: скрипт конкатенации натуральных ключей
    :param p_stg_table_id: id таблицы источника
    :param p_stg_attribute_id: id атрибута таблицы источника
    :param p_update_timestamp_id: id атрибута update_timestamp
    :param p_source_id: id источника
    :param p_etl_value: id etl процесса
    :param p_data_type: тип данных атрибута value
    """
    l_partition_etl=get_table_partition_etl(
        p_table_id=p_attribute_id,
        p_temp_table_id='"'+str(p_attribute_id)+"_3"+'"'
    )
    l_etl="DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+";\n"\
          "CREATE TABLE "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          "idmap."+'"'+str(p_idmap_rk_id)+'"'+" AS idmap_rk\n\t"\
          ",qe."+'"'+str(p_stg_attribute_id)+'"'+" AS attribute_name\n\t"\
          ",qe."+'"'+str(p_update_timestamp_id)+'"'+" AS from_dttm\n\t"\
          ",ROW_NUMBER() OVER (PARTITION BY idmap."+'"'+str(p_idmap_rk_id)+'"'+" ORDER BY qe."+'"'+str(p_update_timestamp_id)+'"'+") AS rnum\n\t"\
          "FROM "+'"'+const('C_STG_SCHEMA').constant_value+'"'+"."+'"'+str(p_stg_table_id)+'"'+" AS qe\n\t"\
          "INNER JOIN "+'"'+const('C_IDMAP_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+'"'+" AS idmap\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND CAST(\n\t\t\t"+p_attribute_concat_nk+"\n\t\t\t||'@@'||\n\t\t\tCAST('"+str(p_source_id)+"' AS VARCHAR(1000)) \n\t\tAS VARCHAR(1000))=idmap."+'"'+str(p_idmap_nk_id)+'"\n);\n'\
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_2"+'"'+";\n" \
          "CREATE TABLE "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_2"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          " crow.idmap_rk\n\t"\
          ",crow.attribute_name\n\t"\
          ",crow.from_dttm\n\t"\
          ",crow.rnum\n\t"\
          "FROM wrk."+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+" AS crow\n\t"\
          "LEFT JOIN wrk."+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+" AS prow\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND crow.idmap_rk=prow.idmap_rk\n\t\t"\
          "AND crow.rnum=prow.rnum+1\n\t\t"\
          "AND crow.attribute_name=prow.attribute_name\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND prow.idmap_rk IS NULL\n);\n"\
          "DROP TABLE IF EXISTS wrk."+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+";\n"\
          "CREATE TABLE wrk."+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          "vers.idmap_rk\n\t"\
          ",vers.attribute_name\n\t"\
          ",vers."+'"'+const('C_FROM_ATTRIBUTE_NAME').constant_value+'"'+"\n\t"\
          ",COALESCE(\n\t\tLEAD(vers."+'"'+const('C_FROM_ATTRIBUTE_NAME').constant_value+'"'+") OVER (PARTITION BY vers.idmap_rk ORDER BY vers.RNUM ASC)-INTERVAL'1'SECOND\n\t\t"\
          ",CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n\t) AS "+'"'+const('C_TO_ATTRIBUTE_NAME').constant_value+'"'+"\n\t"\
          ",lv."+'"'+str(p_attribute_column_id)+'"'+" AS prev_attribute_name\n\t"\
          ",lv."+'"'+str(p_from_dttm_id)+'"'+" AS prev_from_dttm\n\t"\
          ",vers."+'"'+const('C_FROM_ATTRIBUTE_NAME').constant_value+'"'+"- INTERVAL'1'SECOND AS new_to_dttm\n\t"\
          ",lv."+'"'+str(p_etl_id)+'"'+" as prev_etl_id\n\t"\
          "FROM wrk."+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_2"+'"'+" AS vers\n\t"\
          "LEFT JOIN "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+'"'+" AS lv\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND vers.idmap_rk=lv."+'"'+str(p_anchor_rk_id)+'"'+"\n\t\t"\
          "AND lv."+'"'+str(p_to_dttm_id)+'"'+"=CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n\t\t"\
          "AND vers.rnum=1\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND COALESCE(CAST(vers.attribute_name AS VARCHAR(1000)),CAST('###' AS CHAR(3)))<>"\
          "COALESCE(CAST(lv."+'"'+str(p_attribute_column_id)+'"'+" AS VARCHAR(1000)),CAST('###' AS CHAR(3)))\n);\n"\
          "DELETE\n\t"\
          "FROM "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+'"'+" AS attr\n\t"\
          "USING wrk."+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+" AS tmp\n"\
          "WHERE 1=1\n\t"\
          "AND attr."+'"'+str(p_anchor_rk_id)+'"'+"=tmp.idmap_rk\n\t"\
          "AND attr."+'"'+str(p_to_dttm_id)+'"'+"=CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n;\n"\
          "INSERT INTO "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+'"'+"\n(\n\t"\
           +'"'+str(p_anchor_rk_id)+'"'+",\n\t"+'"'+str(p_attribute_column_id)+'"'+",\n\t"\
           +'"'+str(p_from_dttm_id)+'"'+",\n\t"+'"'+str(p_to_dttm_id)+'"'+",\n\t" \
           +'"'+str(p_etl_id)+'"'+"\n)\n\t"\
          "SELECT\n\t"\
          " idmap_rk\n\t"\
          ",prev_attribute_name\n\t"\
          ",prev_from_dttm\n\t"\
          ",new_to_dttm\n\t"\
          ",prev_etl_id\n\t"\
          "FROM "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+"\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND prev_from_dttm IS NOT NULL\n;\n"+l_partition_etl+"\n"\
          "INSERT INTO "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+'"'+"\n(\n\t" \
          +'"'+str(p_anchor_rk_id)+'"'+",\n\t"+'"'+str(p_attribute_column_id)+'"'+",\n\t" \
          +'"'+str(p_from_dttm_id)+'"'+",\n\t"+'"'+str(p_to_dttm_id)+'"'+",\n\t" \
          +'"'+str(p_etl_id)+'"'+"\n)\n\t" \
         "SELECT\n\t"\
         " idmap_rk\n\t,CAST(attribute_name AS "+str(p_data_type)+")\n\t,from_dttm\n\t,to_dttm\n\t,CAST("+str(p_etl_value)+" AS BIGINT)\n\t"\
         "FROM "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+"\n;\n" \
         "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_1"+'"'+";\n" \
         "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_2"+'"'+";\n" \
         "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_attribute_id)+"_3"+'"'+";"

    return l_etl

def get_tie_etl(
        p_tie_id: str,
        p_anchor_rk: str,
        p_link_anchor_rk: str,
        p_from_dttm_id: str,
        p_to_dttm_id: str,
        p_etl_id: str,
        p_idmap_id: str,
        p_idmap_rk_id: str,
        p_idmap_nk_id: str,
        p_idmap_concat: str,
        p_link_idmap_id: str,
        p_link_idmap_rk_id: str,
        p_link_idmap_nk_id: str,
        p_link_idmap_concat: str,
        p_stg_table_id: str,
        p_update_timestamp_id: str,
        p_source_id: str,
        p_etl_value: str
):
    """
    Генерирует etl для таблицы tie

    :param p_tie_id: id таблицы tie
    :param p_anchor_rk: id атрибута rk таблицы tie
    :param p_link_anchor_rk: id атрибута link_rk таблицы tie
    :param p_from_dttm_id: id атрибута from таблицы tie
    :param p_to_dttm_id: id атрибута to таблицы tie
    :param p_etl_id: id атрибута etl таблицы tie
    :param p_idmap_id: id idmap таблицы
    :param p_idmap_rk_id: id атрибута rk таблицы idmap
    :param p_idmap_nk_id: id атрибута nk таблицы idmap
    :param p_idmap_concat: конкатенация натуральных ключей для idmap
    :param p_link_idmap_id: id link_idmap таблицы
    :param p_link_idmap_rk_id: id атрибута rk таблицы link_idmap
    :param p_link_idmap_nk_id: id атрибута nk таблицы link_idmap
    :param p_link_idmap_concat: конкатенация натуральных ключей для link_idmap
    :param p_stg_table_id: id таблицы источника
    :param p_update_timestamp_id: id атрибута update таблицы источника
    :param p_source_id: id источника
    :param p_etl_value: id etl процесса
    """
    l_partition_etl=get_table_partition_etl(
        p_table_id=p_tie_id,
        p_temp_table_id='"'+str(p_tie_id)+"_3"+'"'
    )
    l_etl="DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+";\n"\
          "CREATE TABLE "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          "idmap."+'"'+str(p_idmap_rk_id)+'"'+" AS idmap_rk\n\t"\
          ",l_idmap."+'"'+str(p_link_idmap_rk_id)+'"'+" AS link_idmap_rk\n\t"\
          ",qe."+'"'+str(p_update_timestamp_id)+'"'+" AS from_dttm\n\t"\
          ",ROW_NUMBER() OVER (PARTITION BY idmap."+'"'+str(p_idmap_rk_id)+'"'+" ORDER BY qe."+'"'+str(p_update_timestamp_id)+'"'+") AS rnum\n\t"\
          "FROM "+'"'+const('C_STG_SCHEMA').constant_value+'"'+"."+'"'+str(p_stg_table_id)+'"'+" AS qe\n\t"\
          "INNER JOIN "+'"'+const('C_IDMAP_SCHEMA').constant_value+'"'+"."+'"'+str(p_idmap_id)+'"'+" AS idmap\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND CAST(\n\t\t\t"+p_idmap_concat+"\n\t\t\t||'@@'||\n\t\t\tCAST('"+str(p_source_id)+"' as VARCHAR(1000))\n\t\t as VARCHAR(1000))=idmap."+'"'+str(p_idmap_nk_id)+'"'+"\n\t"\
          "INNER JOIN idmap."+'"'+const('C_IDMAP_SCHEMA').constant_value+'"'+"."+'"'+str(p_link_idmap_id)+'"'+" AS l_idmap\n\t\t"\
          "ON 1=1\n\t\t" \
          "AND CAST(\n\t\t\t"+p_link_idmap_concat+"\n\t\t\t||'@@'||\n\t\t\tCAST('"+str(p_source_id)+"' as VARCHAR(1000))\n\t\t as VARCHAR(1000))=idmap."+'"'+str(p_link_idmap_nk_id)+'"'+"\n);\n" \
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_2"+'"'+";\n"\
          "CREATE TABLE "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_2"+'"'+" AS (\n\t"\
          "SELECT\n\t"\
          " crow.idmap_rk\n\t"\
          ",crow.link_idmap_rk\n\t"\
          ",crow.from_dttm\n\t"\
          ",crow.rnum\n\t"\
          "FROM "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+" AS crow\n\t"\
          "LEFT JOIN "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+" AS prow\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND crow.idmap_rk=prow.idmap_rk\n\t\t"\
          "AND crow.rnum=prow.rnum+1\n\t\t"\
          "AND crow.link_idmap_rk=prow.link_idmap_rk\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND prow.idmap_rk IS NULL\n);"\
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+";\n"\
          "CREATE TABLE "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+" AS \n(\n\t"\
          "SELECT\n\t"\
          " vers.idmap_rk as anchor_rk\n\t"\
          ",vers.link_idmap_rk as link_anchor_rk\n\t"\
          ",vers.from_dttm\n\t"\
          ",COALESCE(\n\t\t"\
          "LEAD(vers.from_dttm) OVER (PARTITION BY vers.idmap_rk ORDER BY vers.RNUM ASC)-INTERVAL'1'SECOND\n\t\t"\
          ",CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n\t"\
          ") AS to_dttm\n\t"\
          ",lv."+'"'+str(p_link_anchor_rk)+'"'+" AS prev_link_anchor_rk\n\t"\
          ",lv."+'"'+str(p_from_dttm_id)+'"'+" AS prev_from_dttm\n\t"\
          ",vers.from_dttm - INTERVAL'1'SECOND AS new_to_dttm\n\t"\
          ",lv."+'"'+str(p_etl_id)+'"'+" as prev_etl_id\n\t"\
          "FROM "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_2"+'"'+" AS vers\n\t"\
          "LEFT JOIN "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+'"'+" AS lv\n\t\t"\
          "ON 1=1\n\t\t"\
          "AND vers.idmap_rk=lv."+'"'+str(p_anchor_rk)+'"'+"\n\t\t"\
          "AND lv."+'"'+str(p_to_dttm_id)+'"'+"=CAST('5999-12-31 00:00:00' AS TIMESTAMP)\n\t\t"\
          "AND vers.rnum=1\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND vers.link_idmap_rk<>COALESCE(lv."+'"'+str(p_link_anchor_rk)+'"'+",CAST(-1 AS BIGINT))\n);\n"\
          "DELETE\n\t"\
          "FROM "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+'"'+" AS attr\n\t"\
          "USING "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+" AS tmp\n"\
          "WHERE 1=1\n\t"\
          "AND attr."+'"'+str(p_anchor_rk)+'"'+"=tmp.anchor_rk\n\t"\
          "AND attr."+'"'+str(p_to_dttm_id)+'"'+"=CAST('5999-12-31 00:00:00' AS TIMESTAMP);\n"\
          "INSERT INTO "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+'"'+"\n(\n\t" \
          +'"'+str(p_anchor_rk)+'"'+",\n\t"+'"'+str(p_link_anchor_rk)+'"'+",\n\t" \
          +'"'+str(p_from_dttm_id)+'"'+",\n\t"+'"'+str(p_to_dttm_id)+'"'+",\n\t" \
          +'"'+str(p_etl_id)+'"'+"\n)\n\t"\
          "SELECT\n\t"\
          " anchor_rk\n\t"\
          ",prev_link_anchor_rk\n\t"\
          ",prev_from_dttm\n\t"\
          ",new_to_dttm\n\t"\
          ",prev_etl_id\n\t"\
          "FROM wrk."+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+"\n\t"\
          "WHERE 1=1\n\t\t"\
          "AND prev_from_dttm IS NOT NULL\n;\n"+l_partition_etl+\
          "INSERT INTO "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+'"'+"\n(\n\t" \
          +'"'+str(p_anchor_rk)+'"'+",\n\t"+'"'+str(p_link_anchor_rk)+'"'+",\n\t" \
          +'"'+str(p_from_dttm_id)+'"'+",\n\t"+'"'+str(p_to_dttm_id)+'"'+",\n\t" \
          +'"'+str(p_etl_id)+'"'+"\n)\n\t" \
          "SELECT\n\t" \
          " anchor_rk\n\t" \
          ",link_anchor_rk\n\t" \
          ",from_dttm\n\t" \
          ",to_dttm\n\t" \
          ",CAST('"+str(p_etl_value)+"'\n\t" \
          "FROM wrk."+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+"\n;\n" \
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_1"+'"'+";\n" \
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_2"+'"'+";\n" \
          "DROP TABLE IF EXISTS "+'"'+const('C_WRK_SCHEMA').constant_value+'"'+"."+'"'+str(p_tie_id)+"_3"+'"'+";"
    return l_etl

def get_table_partition_etl(
        p_table_id: str,
        p_temp_table_id: str
):
    """
    Генерирует скрипт для генерации партиций

    :param p_table_id: id целевой таблицы
    :param p_temp_table_id: id временной таблицы
    """
    l_etl="DO $$\n"\
          "DECLARE\n\t"\
          "v_n_prt_date DATE;\n"\
          "BEGIN\n\t"\
          "FOR v_n_prt_date IN\n\t\t"\
          "SELECT\n\t\t"\
          "DISTINCT\n\t\t"\
          "CAST(DATE_TRUNC('month', tmp."+'"'+const('C_FROM_ATTRIBUTE_NAME').constant_value+'"'+") + "\
          "INTERVAL '1 month - 1 day' AS DATE) AS new_partition_date\n\t\t"\
          "FROM wrk."+'"'+str(p_temp_table_id)+'"'+" tmp\n\t\t"\
          "LEFT JOIN\n\t\t(\n\t\t\t"\
          "SELECT\n\t\t\t"\
          "TO_DATE(SUBSTR(chld.relname,POSITION('_date' IN chld.relname)+5,10),'YYYY-MM-DD') AS partition_date\n\t\t\t"\
          "FROM pg_catalog.pg_class par\n\t\t\t"\
          "LEFT JOIN pg_catalog.pg_inherits inh\n\t\t\t\t"\
          "ON 1=1\n\t\t\t\t"\
          "AND inh.inhparent=par.oid\n\t\t\t"\
          "LEFT JOIN pg_catalog.pg_class chld\n\t\t\t\t"\
          "ON 1=1\n\t\t\t\t"\
          "AND inh.inhrelid=chld.oid\n\t\t\t"\
          "WHERE 1=1\n\t\t\t\t"\
          "AND par.relname='"+str(p_table_id)+"'\n\t\t"\
          ") AS prt\n\t\t\t"\
          "ON 1=1\n\t\t\t"\
          "AND CAST(DATE_TRUNC('month', CAST(tmp."+'"'+const('C_FROM_ATTRIBUTE_NAME').constant_value+'"'+" AS DATE)) "\
          "+ INTERVAL '1 month - 1 day' AS DATE)=prt.partition_date\n\t\t"\
          "WHERE 1=1\n\t\t\t"\
          "AND prt.partition_date IS NULL\n\t"\
          "LOOP\n\t\t"\
          "EXECUTE 'CREATE TABLE "+'"'+str(p_table_id)+"'||v_n_prt_date||"+"'"+'"'+"'"+"||\n\t\t\t"\
          "'PARTITION OF "+'"'+const('C_AM_SCHEMA').constant_value+'"'+"."+'"'+str(p_table_id)+'"'+" FOR VALUES '||\n\t\t\t"\
          "'FROM ('''||CAST(DATE_TRUNC('month',CAST(v_n_prt_date AS DATE)) AS DATE)||' 00:00:00'') TO ('''||v_n_prt_date||' 23:59:59'');';\n\t"\
          "END LOOP;\n"\
          "END;\n"\
          "$$\n"\
          "LANGUAGE 'plpgsql';"

    return l_etl









