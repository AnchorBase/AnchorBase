import dwh_config
import sys
from Constants import *
import SystemObjects
from SystemObjects import *

C_DWH_TYPE=dwh_config.dbms_type


class DBMS:
    """
    Класс по работе СУБД
    """

    def __init__(self, p_dbms_purpose: str =C_DWH, p_dbms_type: str = C_DWH_TYPE, p_dbms_cnct_attr: dict =None):
        """
        Конструктор

        :param p_dbms_purpose: назначение СУБД (по дефолту dwh)
        :param p_dbms_type: тип СУБД (по дефолту берется из meta.dwh -> type)
        :param p_dbms_cnct_attr: параметры подключения
        """
        self._dbms_purpose=p_dbms_purpose
        self._dbms_type=p_dbms_type
        self._dbms_cnct_attr=p_dbms_cnct_attr

    @property
    def dbms_purpose(self) -> str:
        """
        Назначение СУБД. Приводит к нижнему регистру и проверяет на корректность значения
        """
        if self._dbms_purpose not in C_DBMS_PURPOSE_LIST:
            AbaseError(p_error_text="Incorrect purpose of DBMS", p_module="Driver", p_class="DBMS",
                       p_def="dbms_purpose").raise_error()
        else:
            return self._dbms_purpose.lower()

    @property
    def dbms_type(self) -> str:
        """
        Тип СУБД. Проверяет, умеет ли AnchorBase работать с таким СУБД. Приводит к нижнему регистру
        """
        if self.dbms_purpose==C_SOURCE:
            if self._dbms_type not in C_AVAILABLE_SOURCE_LIST:
                AbaseError(p_error_text="DBMS is not supported by AnchorBase", p_module="Driver", p_class="DBMS", p_def="dbms_type").raise_error()
        if self.dbms_purpose==C_DWH:
            if self._dbms_type not in C_AVAILABLE_DWH_LIST:
                AbaseError(p_error_text="DBMS is not supported by AnchorBase", p_module="Driver", p_class="DBMS", p_def="dbms_type").raise_error()

        return self._dbms_type.lower()

    @property
    def dbms_cnct_attr(self):
        """
        Параметры подключения к СУБД. Проверяет ключи словаря на полноту указанных параметров
        """
        # если пустой, ничего не делаем
        if self._dbms_cnct_attr is None:
            return self._dbms_cnct_attr

        l_cnct_params = C_CNCT_PARAMS # фиксированный список параметров подключения
        l_cnct_attr_keys_list = list(self._dbms_cnct_attr.keys()) # определяем ключи словаря с параметрами подключения
        # проверяем, что все ключи указаны корректно
        for i_cnct_attr_keys_list in l_cnct_attr_keys_list:
            if i_cnct_attr_keys_list not in l_cnct_params:
                AbaseError(p_error_text="Parametr of connection is incorrect", p_module="Driver", p_class="DBMS",
                           p_def="dbms_cnct_attr").raise_error()
        # проверка, что все необходимые параметры указаны
        for i_cnct_params in l_cnct_params:
            if i_cnct_params not in l_cnct_attr_keys_list:
                AbaseError(p_error_text="Parametr of connection "+i_cnct_params+" is empty", p_module="Driver", p_class="DBMS",
                           p_def="dbms_cnct_attr").raise_error()
        return self._dbms_cnct_attr


class DataType(DBMS):
    """
    Класс типа данных СУБД
    """
    def __init__(self,
                 p_data_type_name: str,
                 p_data_type_length: int =None,
                 p_data_type_scale: int =None,
                 p_dbms_purpose: str =C_DWH,
                 p_dbms_type: str =C_DWH_TYPE
    ):
        """
        Конструктор

        :param p_dbms_purpose: назначение СУБД (по дефолту dwh)
        :param p_dbms_type: тип СУБД (по дефолту берется из meta.dwh -> type)
        :param p_data_type_name: наименование типа данных
        :param p_data_type_length: длина
        :param p_data_type_scale: количество знаков после запятой
        """
        super().__init__(p_dbms_purpose=p_dbms_purpose, p_dbms_type=p_dbms_type)
        self._data_type_name=p_data_type_name
        self._data_type_length=p_data_type_length
        self._data_type_scale=p_data_type_scale

    @property
    def data_type_name(self) -> str:
        """
        Наименование типа данных.
        Проверка, есть ли указанный тип данных в СУБД/Умеет ли AnchorBase с ним работать.
        Приводит к верхнему регистру
        """
        self._data_type_name=self._data_type_name.lower()
        l_datatype_list=C_DBMS_COMPONENTS.get(self.dbms_type,None).get(C_DATATYPE,None)
        if self._data_type_name not in l_datatype_list:
            AbaseError(p_error_text="Data type "+self._data_type_name+" is incorrect", p_module="Driver",
                       p_class="DataType", p_def="data_type_name").raise_error()
        return self._data_type_name.upper()

    @property
    def data_type_length(self):
        """
        Длина типа данных. Проверка на целое число
        """
        # если пусто, ничего не делаем
        if self._data_type_length is None:
            return self._data_type_length
        elif type(self._data_type_length) is not int:
            AbaseError(p_error_text="Data type is incorrect (length)", p_module="Driver",
                       p_class="DataType", p_def="data_type_length").raise_error()
        else:
            return self._data_type_length

    @property
    def data_type_scale(self):
        """
        Количество цифр после запятой. Проверка на целое число
        """
        # если пусто, ничего не делаем
        if self._data_type_scale is None:
            return self._data_type_scale
        elif type(self._data_type_scale) is not int:
            AbaseError(p_error_text="Data format is incorrect (quantity of numbers after spot)", p_module="Driver",
                       p_class="DataType", p_def="data_type_scale").raise_error()
        else:
            return self._data_type_scale
    @property
    def data_type_sql(self):
        """
        SQL-выражение типа данных
        """
        l_data_type_ddl=self.data_type_name
        if self.data_type_length is not None:
            l_data_type_ddl=l_data_type_ddl+"("+str(self.data_type_length)
            if self.data_type_scale is not None:
                l_data_type_ddl=l_data_type_ddl+","+str(self.data_type_scale)
            l_data_type_ddl=l_data_type_ddl+")"
        return l_data_type_ddl
