# coding=utf-8
import psycopg2
import psycopg2.extensions
import sys
import copy
import datetime
import json
import Metadata
import Support

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
    try: # проверяем подключение
        cnct = psycopg2.connect(
            dbname=p_database,
            user=p_user,
            password=p_password,
            host=p_server,
            port=p_port
        )
    except psycopg2.OperationalError as e:
        sys.exit(e) #TODO: реализовать вывод ошибок, как сделал Рустем
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
        sys.exit(e) #TODO: реализовать вывод ошибок, как сделал Рустем
    finally:
        crsr.close()
        cnct.close()
    return l_query_output









