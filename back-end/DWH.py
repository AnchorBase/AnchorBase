"""
Работа с ХД
"""
import sys
import dwh_config
from SystemObjects import Constant as const
import Driver
import Postgresql as pgsql
import FileWorker as fw

class Connection:
    """
    Подключение к ХД
    """
    l_server=dwh_config.server
    l_database=dwh_config.database
    l_user=dwh_config.user
    l_password=dwh_config.password
    l_port=dwh_config.port
    l_dbms_type=dwh_config.dbms_type
    def __init__(self,
                 p_server: str=l_server,
                 p_database: str =l_database,
                 p_user: str =l_user,
                 p_password: str =l_password,
                 p_port: int =l_port,
                 p_dbms_type: str=l_dbms_type
    ):
        """
        Конструктор

        :param p_server: сервер
        :param p_database: наименование бд postgresql
        :param p_user: пользователь
        :param p_password: пароль
        :param p_port: порт (по умолчанию 5432)
        :param p_dbms_type: тип СУБД
        """
        self._server=p_server
        self._database=p_database
        self._user=p_user
        self._password=p_password
        self._port=p_port
        self._dbms_type=p_dbms_type

    @property
    def server(self):
        """
        Хост сервера ХД
        """
        return self._server

    @property
    def database(self):
        """
        Наименование базы данных
        """
        return self._database

    @property
    def user(self):
        """
        Пользователь
        """
        return self._user

    @property
    def password(self):
        """
        Пароль
        """
        return self._password

    @property
    def port(self):
        """
        Порт
        """
        return self._port

    @property
    def dbms_type(self):
        """
        Тип СУБД
        """
        l_dbms_type=self._dbms_type.lower()
        if l_dbms_type not in const('C_AVAILABLE_DWH_LIST').constant_value:
            sys.exit("AnchorBase не умеет работать с СУБД "+l_dbms_type) #TODO: реализовать вывод ошибок, как сделал Рустем
        return l_dbms_type

    @property
    def dbms(self):
        """
        Модуль СУБД
        """
        if self.dbms_type==const('C_POSTGRESQL').constant_value:
            return pgsql


    def sql_exec(self, p_sql: str, p_result: int =1):
        """
        Выполнение запроса на стороне ХД

        :param p_sql: SQL-выражение
        :param p_result: Признак наличия результата запроса (по умолчанию 1)
        """
        l_sql_result=self.dbms.sql_exec(
            p_server=self.server,
            p_database=self.database,
            p_user=self.user,
            p_password=self.password,
            p_port=self.port,
            p_sql=p_sql,
            p_result=p_result
        )
        return l_sql_result



    def connection_checker(self):
        """
        Тест подключения
        """
        self.dbms.sql_exec(
            p_server=self.server,
            p_database=self.database,
            p_user=self.user,
            p_password=self.password,
            p_port=self.port,
            p_sql="select 1;"
        )

    def set_dwh_param(self):
        """
        Записывает/Изменяет параметры подключения к ХД
        """
        # тест подключения
        self.connection_checker()
        # переписываем dwh_config
        l_config='server = "'+self.server+'"\n' \
                 'database = "'+self.database+'"\n' \
                 'user = "'+self.user+'"\n' \
                 'password = "'+self.password+'"\n' \
                 'port = "'+str(self.port)+'"\n' \
                 'dbms_type = "'+self.dbms_type+'"'
        fw.File(
            p_file_path=const('C_CONFIG_FILE_PATH').constant_value,
            p_file_body=l_config
        ).write_file()

class Job:
    """
    Загружает данные
    """
    def __init__(self, p_entity_id: list =None):
        """
        Конструктор
        """
        self._entity_id=p_entity_id


def get_values_sql(p_data_frame: list, p_cast_list: dict):
    """
    Возвращает преобразованный data frame в конструкцию values (...,...),

    :param p_data_frame: data frame
    :param p_cast_list: словарь с порядковыми номерами атрибутов и конструкцией cast для каждого атрибута
    """
    l_sql=""
    for i_row in p_data_frame:
        i=0
        l_sql=l_sql+"("
        for i_column in i_row:
            if p_cast_list.get(i,None) is None:
                sys.exit("Нет указания cast для атрибута "+str(i)) #TODO: реализовать вывод ошибок, как сделал Рустем
            i_column="'"+str(i_column).replace("'","''")+"'" # экранируем одинарную кавычку ' в строке
            l_sql=l_sql+p_cast_list.get(i,None).replace(str(i),i_column,1)+","
            i=i+1
        l_sql=l_sql[:-1]+"),"
    return l_sql[:-1]