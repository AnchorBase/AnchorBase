

"""
Константы, используемые в программе
"""
# НЕ ПЛОДИТЬ КОНСТАНТЫ!!!
# ПЕРЕД ТЕМ КАК ДОБАВИТЬ КОНСТАНТУ ТРЕБУЕТЯ ПРОВЕРИТЬ, ЧТО ЕЕ ЗНАЧЕНИЕ НЕ ПОВТОРЯЕТСЯ В ДРУГОЙ КОНСТАНТЕ


# основные понятия
C_TABLE = "table" # таблица
C_NAME = "name" # наименование
C_DDL = "ddl"
C_VIEW = "view"
C_ETL = "etl"
C_DATATYPE = "datatype" # тип данных
C_LENGTH = "length" # длина значения атрибута
C_SCALE = "scale" # количество знаков после запятой
C_DWH = "dwh" # СУБД
C_SOURCE = "source"
C_SERVER = "server"
C_DATABASE = "database"
C_USER = "user"
C_PASSWORD = "password"
C_PORT = "port"
C_TEMPLATE_FOLDER = "template_folder" # папка, в которой лежат шаблоны DDL/ETL
C_TEMP_TABLE = "temp_table" # временная таблица
C_CAST = "CAST" # операция CAST
C_CONCAT_SYMBOL = "@@" #символ для конкатенации

# СУБД
C_MSSQL = "mssql"
C_POSTGRESQL = "postgresql"
C_AVAILABLE_SOURCE_LIST = [C_MSSQL] # фиксированный список СУБД, с которым AnchorBase умеет работать как с источником
C_AVAILABLE_DWH_LIST = [C_POSTGRESQL] # фиксированный список СУБД, с которым AnchorBase умеет работать как с DWH
C_DBMS_PURPOSE_LIST = [C_SOURCE,C_DWH] # фиксированный список назначений СУБД (источник, ХД)
C_CNCT_PARARMS = [  # фиксированный список параметров подключения
    C_SERVER,
    C_DATABASE,
    C_USER,
    C_PASSWORD,
    C_PORT
]

# типы данных
C_SMALLINT = "smallint"
C_INTEGER = "integer"
C_INT = "int"
C_BIGINT = "bigint"
C_DECIMAL = "decimal"
C_NUMERIC = "numeric"
C_REAL = "real"
C_DOUBLE = "double"
C_SMALLSERIAL = "smallserial"
C_SERIAL = "serial"
C_BIGSERIAL = "bigserial"
C_MONEY = "money"
C_CHARACTER = "character"
C_CHAR = "char"
C_VARCHAR = "varchar"
C_TEXT = "text"
C_BYTEA = "bytea"
C_TIMESTAMP = "timestamp"
C_DATE = "date"
C_TIME = "time"
C_INTERVAL = "interval"
C_BOOLEAN = "boolean"

# PostgreSQL
C_POSTGRESQL_DATA_TYPE_LIST = [ # фиксированный список типов данных для PostgreSQL, с которыми умеет работать AnchorBase
     C_SMALLINT
    ,C_INTEGER
    ,C_INT
    ,C_BIGINT
    ,C_DECIMAL
    ,C_NUMERIC
    ,C_REAL
    ,C_DOUBLE
    ,C_SMALLSERIAL
    ,C_SERIAL
    ,C_BIGSERIAL
    ,C_MONEY
    ,C_CHARACTER
    ,C_CHAR
    ,C_VARCHAR
    ,C_TEXT
    ,C_BYTEA
    ,C_TIMESTAMP
    ,C_DATE
    ,C_TIME
    ,C_INTERVAL
    ,C_BOOLEAN
]

C_POSTGRESQL_TEMPLATE_FOLDER = "POSTGRESQL" # наименование папки с шаблонами SQL для PostgreSQL

# компоненты СУБД: типы данных и т.д.
C_DBMS_COMPONENTS = {
    C_POSTGRESQL:{
        C_DATATYPE:C_POSTGRESQL_DATA_TYPE_LIST,
        C_TEMPLATE_FOLDER:C_POSTGRESQL_TEMPLATE_FOLDER
    }
}

# свойства основных понятий
C_NAME_TABLE = C_TABLE+"_"+C_NAME # наименование таблицы

# Наименование типов таблиц
#!!! При создании нового типа таблицы добавить константу в список C_TABLE_TYPE_LIST
C_ANCHOR_TABLE_TYPE_NAME = "anchor" # наименование типа таблицы anchor
C_ATTRIBUTE_TABLE_TYPE_NAME = "attribute" # наименование типа таблицы attribute
C_TIE_TABLE_TYPE_NAME = "tie" # наименование типа таблицы tie
C_IDMAP_TABLE_TYPE_NAME = "idmap" # наименование типа таблицы idmap
C_QUEUE_TABLE_TYPE_NAME = "queue" # наименование типа таблицы queue
C_LINK_IDMAP_TABLE_TYPE_NAME = "link_idmap" # наименование типа таблицы idmap связанной сущности (для ETL в tie)

C_TABLE_TYPE_LIST = [ # фиксированный список типов таблиц
    C_ANCHOR_TABLE_TYPE_NAME,
    C_ATTRIBUTE_TABLE_TYPE_NAME,
    C_TIE_TABLE_TYPE_NAME,
    C_QUEUE_TABLE_TYPE_NAME,
    C_IDMAP_TABLE_TYPE_NAME
]

C_SOURCE_TABLE_TYPE_LIST = [ # фиксированный список типов таблиц источников
    C_ANCHOR_TABLE_TYPE_NAME,
    C_ATTRIBUTE_TABLE_TYPE_NAME,
    C_TIE_TABLE_TYPE_NAME,
    C_QUEUE_TABLE_TYPE_NAME,
    C_IDMAP_TABLE_TYPE_NAME,
    C_LINK_IDMAP_TABLE_TYPE_NAME
]

# Наименование типов атрибутов таблиц
#!!! При создании нового типа атрибута добавить константу в список C_ATTRIBUTE_TABLE_TYPE_LIST
C_RK_TYPE_NAME = "rk" # наименование типа атрибута - суррогатный ключ
C_NK_TYPE_NAME = "nk" # наименование типа атрибута - натуральный ключ
C_SOURCE_TYPE_NAME = "source" # наименование типа атрибута - источник
C_ETL_TYPE_NAME = "etl" # наименование типа атрибута - идентификатор процесса загрузки данных
C_VALUE_TYPE_NAME = "value" # наименование типа атрибута - значение атрибута в таблице типа attribute
C_FROM_TYPE_NAME = "from" # наименование типа атрибута - дата начала действия записи
C_TO_TYPE_NAME = "to" # наименование типа атрибута - дата окончания действия записи
C_LINK_RK_TYPE_NAME = "link_rk" # наименование типа атрибута - суррогатный ключ связанной сущности в таблице tie
C_LINK_NK_TYPE_NAME = "link_nk" # наименование типа атрибута - натуральный ключ связанной сущности в таблице tie
C_QUEUE_ATTR_TYPE_NAME = "queue_attr" # наименование типа атрибута - атрибут таблицы queue
C_UPDATE_TYPE_NAME = "update" # наименование типа атрибута - атрибут, хранящий инкремент, таблицы queue

C_ATTRIBUTE_TABLE_TYPE_LIST = [
    C_RK_TYPE_NAME,
    C_NK_TYPE_NAME,
    C_SOURCE_TYPE_NAME,
    C_ETL_TYPE_NAME,
    C_VALUE_TYPE_NAME,
    C_FROM_TYPE_NAME,
    C_TO_TYPE_NAME,
    C_QUEUE_ATTR_TYPE_NAME,
    C_UPDATE_TYPE_NAME
]

C_SOURCE_ATTRIBUTE_TABLE_TYPE_LIST = [
    C_RK_TYPE_NAME,
    C_NK_TYPE_NAME,
    C_SOURCE_TYPE_NAME,
    C_ETL_TYPE_NAME,
    C_VALUE_TYPE_NAME,
    C_FROM_TYPE_NAME,
    C_TO_TYPE_NAME,
    C_LINK_RK_TYPE_NAME,
    C_LINK_NK_TYPE_NAME,
    C_QUEUE_ATTR_TYPE_NAME,
    C_UPDATE_TYPE_NAME
]

# свойства атрибутов таблиц
C_NAME_RK_ATTRIBUTE = C_RK_TYPE_NAME+"_"+C_NAME # наименование атрибута суррогатного ключа
C_NAME_VALUE_ATTRIBUTE = C_VALUE_TYPE_NAME+"_"+C_NAME # наименование атрибута, содержащего значения, в таблице attribute
C_NAME_LINK_RK_ATTRIBUTE = C_LINK_RK_TYPE_NAME+"_"+C_NAME # наименование атрибута суррогатного ключа связанной сущности
C_NAME_NK_ATTRIBUTE = C_NK_TYPE_NAME+"_"+C_NAME # наименование атрибута натурального ключа
C_NAME_QUEUE_ATTR_ATTRIBUTE = C_QUEUE_ATTR_TYPE_NAME+"_"+C_NAME # наименование атрибута таблицы queue

C_TABLE_ANCHOR_ATTR_TYPE_LIST = [ # фиксированный список необходимых атрибутов anchor таблицы
    C_RK_TYPE_NAME,
    C_SOURCE_TYPE_NAME,
    C_ETL_TYPE_NAME
]
C_TABLE_ATTRIBUTE_ATTR_TYPE_LIST = [ # фиксированный список необходимых атрибутов  attribute таблицы
    C_RK_TYPE_NAME,
    C_VALUE_TYPE_NAME,
    C_FROM_TYPE_NAME,
    C_TO_TYPE_NAME,
    C_ETL_TYPE_NAME
]
C_TABLE_TIE_ATTR_TYPE_LIST = [ # фиксированный список необходимых атрибутов  tie таблицы
    C_RK_TYPE_NAME,
    C_LINK_RK_TYPE_NAME,
    C_FROM_TYPE_NAME,
    C_TO_TYPE_NAME,
    C_ETL_TYPE_NAME
]
C_TABLE_QUEUE_ATTR_TYPE_LIST = [ # фиксированный список необходимых атрибутов  queue таблицы
    C_QUEUE_ATTR_TYPE_NAME,
    C_UPDATE_TYPE_NAME,
    C_ETL_TYPE_NAME
]
C_TABLE_IDMAP_ATTR_TYPE_LIST = [ # фиксированный список необходимых атрибутов  idmap таблицы
    C_RK_TYPE_NAME,
    C_NK_TYPE_NAME,
    C_ETL_TYPE_NAME
]

# DDL компоненты
C_TABLE_ANCHOR_TEMPLATE_VARIABLES_DICT = { # список переменных для замены в шаблоне таблицы anchor
    C_TABLE:"&&anchor_id",
    C_NAME_TABLE:"&&anchor_name",
    C_RK_TYPE_NAME:"&&anchor_rk_id",
    C_NAME_RK_ATTRIBUTE:"&&anchor_rk_name",
    C_SOURCE_TYPE_NAME:"&&source_id",
    C_ETL_TYPE_NAME:"&&etl_id"
}
C_TABLE_ATTRIBUTE_TEMPLATE_VARIABLES_DICT = { # список переменных для замены в шаблоне таблицы attribute
    C_TABLE:"&&attribute_id",
    C_NAME_TABLE:"&&attribute_name",
    C_RK_TYPE_NAME:"&&anchor_rk_id",
    C_NAME_RK_ATTRIBUTE:"&&anchor_rk_name",
    C_VALUE_TYPE_NAME:"&&attribute_name_id_and_datatype",
    C_NAME_VALUE_ATTRIBUTE:"&&atribute_column",
    C_FROM_TYPE_NAME:"&&from_dttm_id",
    C_TO_TYPE_NAME:"&&to_dttm_id",
    C_ETL_TYPE_NAME:"&&etl_id"
}
C_TABLE_TIE_TEMPLATE_VARIABLES_DICT = { # список переменных для замены в шаблоне таблицы tie
    C_TABLE:"&&tie_id",
    C_NAME_TABLE:"&&tie_name",
    C_RK_TYPE_NAME:"&&anchor_rk_id",
    C_NAME_RK_ATTRIBUTE:"&&anchor_rk_name",
    C_LINK_RK_TYPE_NAME:"&&link_anchor_rk_id",
    C_NAME_LINK_RK_ATTRIBUTE:"&&link_anchor_column_name",
    C_FROM_TYPE_NAME:"&&from_dttm_id",
    C_TO_TYPE_NAME:"&&to_dttm_id",
    C_ETL_TYPE_NAME:"&&etl_id"
}
C_TABLE_IDMAP_TEMPLATE_VARIABLES_DICT = { # список переменных для замены в шаблоне таблицы tie
    C_TABLE:"&&idmap_id",
    C_NAME_TABLE:"&&idmap_name",
    C_RK_TYPE_NAME:"&&idmap_rk_id",
    C_NAME_RK_ATTRIBUTE:"&&idmap_rk_name",
    C_NK_TYPE_NAME:"&&idmap_nk_id",
    C_NAME_NK_ATTRIBUTE:"&&idmap_nk_name",
    C_ETL_TYPE_NAME:"&&etl_id"
}
C_TABLE_QUEUE_TEMPLATE_VARIABLES_DICT = { # список переменных для замены в шаблоне таблицы queue
    C_TABLE:"&&queue_table_id",
    C_NAME_TABLE:"&&queue_table_name",
    C_QUEUE_ATTR_TYPE_NAME:"&&queue_table_attrs_and_datatypes",
    C_NAME_QUEUE_ATTR_ATTRIBUTE:"@queue_attrs_aliace",
    C_UPDATE_TYPE_NAME:"&&update_timestamp_id",
    C_ETL_TYPE_NAME:"&&etl_id",
}

# Шаблоны
C_TEMPLATE_TYPE_LIST = [ # фиксированный список типов шаблонов
    C_DDL,
    C_ETL,
    C_VIEW
]
C_TEMPLATE_POSTFIX = "TEMPLATE" # постфикс файлов-шаблонов
C_TEMPLATE_FILE_TYPE = ".sql" # расширение файлов-шаблонов

C_DDL_TEMPLATE = C_DDL.upper()+"_"+C_TEMPLATE_POSTFIX # наименование папки с шаблонами DDL
C_ETL_TEMPLATE = C_ETL.upper()+"_"+C_TEMPLATE_POSTFIX # наименование папки с шаблонами ETL
C_VIEW_TEMPLATE = C_VIEW.upper()+"_"+C_TEMPLATE_POSTFIX # наименование папки с шаблонами VIEW

C_CONCAT_NK_VAR = "&&concat_nkey_sql" # переменная в etl-шаблоне
C_CONCAT_LINK_NK_VAR="&&link_concat_nkey_sql" # переменная в etl-шаблоне
C_SOURCE_VAR = "&&source_id" # переменная в etl-шаблоне

C_ANCHOR_ETL_TEMPLATE_VARIABLES = {
    C_TABLE:{
        C_ANCHOR_TABLE_TYPE_NAME:"&&anchor_id",
        C_RK_TYPE_NAME:"&&anchor_rk_id",
        C_SOURCE_TYPE_NAME:"&&source_system_id",
        C_ETL_TYPE_NAME:"&&etl_id",
    },
    C_IDMAP_TABLE_TYPE_NAME:{
        C_IDMAP_TABLE_TYPE_NAME:"&&idmap_id",
        C_RK_TYPE_NAME:"&&idmap_rk_id",
        C_NK_TYPE_NAME:"&&idmap_nk_id"
    }
}

C_ATTRIBUTE_ETL_TEMPLATE_VARIABLES = {
    C_TABLE:{
        C_ATTRIBUTE_TABLE_TYPE_NAME:"&&attribute_id",
        C_RK_TYPE_NAME:"&&anchor_rk_id",
        C_ETL_TYPE_NAME:"&&etl_id",
        C_VALUE_TYPE_NAME:"&&attribute_column_id",
        C_FROM_TYPE_NAME:"&&from_dttm_id",
        C_TO_TYPE_NAME:"&&to_dttm_id"
    },
    C_IDMAP_TABLE_TYPE_NAME:{
        C_IDMAP_TABLE_TYPE_NAME:"&&idmap_id",
        C_RK_TYPE_NAME:"&&idmap_rk_id",
        C_NK_TYPE_NAME:"&&idmap_nk_id"
    },
    C_QUEUE_TABLE_TYPE_NAME:{
        C_QUEUE_TABLE_TYPE_NAME:"&&stg_table_id",
        C_NK_TYPE_NAME:C_CONCAT_NK_VAR,
        C_UPDATE_TYPE_NAME:"&&update_timestamp_id",
        C_QUEUE_ATTR_TYPE_NAME:"&&stg_attribute_column_id",
        C_SOURCE:"&&source_id"
    },
    C_TEMP_TABLE:{
        1:"&&temp_rnum_table_id",
        2:"&&temp_change_table_id",
        3:"&&temp_insert_table_id"
    }

}

# Компоненты таблицы: атрибуты, DDL компоненты, ETL компоненты в соответствии с типом таблицы
C_TABLE_ATTRIBUTES = "table_attributes"
C_TABLE_DDL_COMPONENTS = "table_ddl"
C_TABLE_ETL_COMPONENTS = "table_etl"
C_TABLE_COMPONENTS = {
    C_ANCHOR_TABLE_TYPE_NAME:{
        C_TABLE_ATTRIBUTES:C_TABLE_ANCHOR_ATTR_TYPE_LIST,
        C_TABLE_DDL_COMPONENTS:C_TABLE_ANCHOR_TEMPLATE_VARIABLES_DICT,
        C_TABLE_ETL_COMPONENTS:C_ANCHOR_ETL_TEMPLATE_VARIABLES
    },
    C_ATTRIBUTE_TABLE_TYPE_NAME:{
        C_TABLE_ATTRIBUTES:C_TABLE_ATTRIBUTE_ATTR_TYPE_LIST,
        C_TABLE_DDL_COMPONENTS:C_TABLE_ATTRIBUTE_TEMPLATE_VARIABLES_DICT,
        C_TABLE_ETL_COMPONENTS:C_ATTRIBUTE_ETL_TEMPLATE_VARIABLES
    },
    C_TIE_TABLE_TYPE_NAME:{
        C_TABLE_ATTRIBUTES:C_TABLE_TIE_ATTR_TYPE_LIST,
        C_TABLE_DDL_COMPONENTS:C_TABLE_TIE_TEMPLATE_VARIABLES_DICT
    },
    C_IDMAP_TABLE_TYPE_NAME:{
        C_TABLE_ATTRIBUTES:C_TABLE_IDMAP_ATTR_TYPE_LIST,
        C_TABLE_DDL_COMPONENTS:C_TABLE_IDMAP_TEMPLATE_VARIABLES_DICT
    },
    C_QUEUE_TABLE_TYPE_NAME:{
        C_TABLE_ATTRIBUTES:C_TABLE_QUEUE_ATTR_TYPE_LIST,
        C_TABLE_DDL_COMPONENTS:C_TABLE_QUEUE_TEMPLATE_VARIABLES_DICT
    }
}


def get_constant_list():
    """
    Возвращает список всех констант и их значения
    """
    return globals()
