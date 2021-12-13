

"""
Константы, используемые в программе
"""
# НЕ ПЛОДИТЬ КОНСТАНТЫ!!!
# ПЕРЕД ТЕМ КАК ДОБАВИТЬ КОНСТАНТУ ТРЕБУЕТЯ ПРОВЕРИТЬ, ЧТО ЕЕ ЗНАЧЕНИЕ НЕ ПОВТОРЯЕТСЯ В ДРУГОЙ КОНСТАНТЕ


# основные понятия
C_ENTITY = "entity" # сущность
C_LINK_ENTITY = "link_entity"
C_ENTITY_ATTRIBUTE = "entity_attribute" # атрибут сущности
C_TABLE = "table" # таблица
C_COLUMN = "column" # колонка
C_ATTRIBUTE = "attribute"
C_SCHEMA = "schema" # схема
C_NAME = "name" # наименование
C_SOURCE_NAME = "source_name" # наименование на источнике
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
C_CONCAT_SYMBOL = "@@" #символ для конкатенаци
C_STATUS="status"
C_STATUS_IN_PROGRESS="in progress"
C_STATUS_FAIL="fail"
C_STATUS_SUCCESS="success"
C_STATUS_START="start"
C_START_DATETIME="start_datetime"
C_END_DATETIME="end_datetime"
C_PACKAGE="package"
C_ERROR="error"

# Наименование типов таблиц
#!!! При создании нового типа таблицы добавить константу в список C_TABLE_TYPE_LIST
C_ANCHOR_TABLE_TYPE_NAME = "anchor" # наименование типа таблицы anchor
C_ATTRIBUTE_TABLE_TYPE_NAME = "attribute" # наименование типа таблицы attribute
C_TIE_TABLE_TYPE_NAME = "tie" # наименование типа таблицы tie
C_IDMAP_TABLE_TYPE_NAME = "idmap" # наименование типа таблицы idmap
C_QUEUE_TABLE_TYPE_NAME = "queue" # наименование типа таблицы queue
C_LINK_IDMAP_TABLE_TYPE_NAME = "link_idmap" # наименование типа таблицы idmap связанной сущности (для ETL в tie)

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

C_SOURCE_ATTRIBUTE_NAME="source_system_id"
C_ETL_ATTRIBUTE_NAME="etl_id"
C_FROM_ATTRIBUTE_NAME="from_dttm"
C_TO_ATTRIBUTE_NAME="to_dttm"
C_UPDATE_TIMESTAMP_NAME = "update_timestamp"

# потсфиксы таблиц
C_TABLE_NAME_POSTFIX={
    C_ANCHOR_TABLE_TYPE_NAME:"_an",
    C_ATTRIBUTE_TABLE_TYPE_NAME:"_attr",
    C_TIE_TABLE_TYPE_NAME:"_tie",
    C_IDMAP_TABLE_TYPE_NAME:"_idmap",
    C_QUEUE_TABLE_TYPE_NAME:"_queue"
}

# настройки приложения
C_CONFIG_FILE_PATH = "dwh_config.py" # путь до файла с конфигами подключения к ХД
C_MSSQL_DRIVER_MACOS_PATH = "/usr/local/lib/libtdsodbc.so" # расположение драйвера в MacOS
C_TDS_VERSION = '7.3' # версия TDS для pyodbc

# метаданные
# !!! При добавлении новой переменной метаданных, добавить ее в список C_META_TABLES
C_SOURCE_META = "source" # наименование таблицы с параметрами источников
C_QUEUE_COLUMN = C_QUEUE_TABLE_TYPE_NAME+"_"+C_COLUMN
C_IDMAP_COLUMN = C_IDMAP_TABLE_TYPE_NAME+"_"+C_COLUMN
C_ANCHOR_COLUMN = C_ANCHOR_TABLE_TYPE_NAME+"_"+C_COLUMN
C_ATTRIBUTE_COLUMN = C_ATTRIBUTE_TABLE_TYPE_NAME+"_"+C_COLUMN
C_TIE_COLUMN = C_TIE_TABLE_TYPE_NAME+"_"+C_COLUMN
C_ENTITY_COLUMN = C_ENTITY+"_"+C_COLUMN
C_LINK_ENTITY_COLUMN = C_LINK_ENTITY+"_"+C_COLUMN
C_ENTITY_LINK_COLUMN = C_ENTITY+"_link"+C_COLUMN
C_INCREMENT = "increment"
C_QUEUE_INCREMENT=C_QUEUE_TABLE_TYPE_NAME+"_"+C_INCREMENT
C_QUEUE_ETL="queue_etl"
C_IDMAP_ETL="idmap_etl"
C_ANCHOR_ETL="anchor_etl"
C_ATTRIBUTE_ETL="attribute_etl"
C_TIE_ETL="tie_etl"

C_META_TABLES = [
    C_SOURCE_META,
    C_ENTITY,
    C_ENTITY_COLUMN,
    C_QUEUE_TABLE_TYPE_NAME,
    C_QUEUE_COLUMN,
    C_IDMAP_TABLE_TYPE_NAME,
    C_IDMAP_COLUMN,
    C_ANCHOR_TABLE_TYPE_NAME,
    C_ANCHOR_COLUMN,
    C_ATTRIBUTE_TABLE_TYPE_NAME,
    C_ATTRIBUTE_COLUMN,
    C_TIE_TABLE_TYPE_NAME,
    C_TIE_COLUMN,
    C_QUEUE_COLUMN,
    C_QUEUE_INCREMENT,
    C_ETL,
    C_QUEUE_ETL,
    C_IDMAP_ETL,
    C_ANCHOR_ETL,
    C_ATTRIBUTE_ETL,
    C_TIE_ETL
]

C_STG_SCHEMA="stg"
C_IDMAP_SCHEMA="idmap"
C_AM_SCHEMA="am"
C_WRK_SCHEMA="wrk"

C_SCHEMA_TABLE_TYPE = { # наименование схемы в соответствии с типом таблицы
    C_QUEUE_TABLE_TYPE_NAME:C_STG_SCHEMA,
    C_IDMAP_TABLE_TYPE_NAME:C_IDMAP_SCHEMA,
    C_ANCHOR_TABLE_TYPE_NAME:C_AM_SCHEMA,
    C_ATTRIBUTE_TABLE_TYPE_NAME:C_AM_SCHEMA,
    C_TIE_TABLE_TYPE_NAME:C_AM_SCHEMA
}

# часто используемые атрибуты объектов метаданных
C_DELETED="deleted" # признак удаления

# атрибуты метаданных

C_NOT_NULL = "not null" # наименование признака обязательного атрибута
C_TYPE_VALUE = "type" # наименование признака - тип атрибута
C_PK = "pk" # наименование признака ключ у атрибута метаданных
C_DESC="description"
C_SOURCE_ID="source_id"
C_ATTRIBUTE_NK = "column_nk"
C_ATTRIBUTE_VALUE = "column_value"
C_LINK_ATTRIBUTE_NK = "link_column_nk"

C_QUEUE_COLUMN = C_QUEUE_TABLE_TYPE_NAME+"_"+C_COLUMN


C_SOURCE_META_ATTRIBUTES = { # необходимые атрибуты источника для метаданных
    C_SERVER:{C_NOT_NULL:1,C_TYPE_VALUE:"str", C_PK:0},
    C_DATABASE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_USER:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_PASSWORD:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_PORT:{C_NOT_NULL:1,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_DESC:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_SOURCE_ID:{C_NOT_NULL:1,C_TYPE_VALUE:"int",C_PK:1} # ключ источника типа int для source_system_id
}


C_ENTITY_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_DESC:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY+"_"+C_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_SOURCE_META:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_QUEUE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_IDMAP_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ANCHOR_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ATTRIBUTE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_TIE_TABLE_TYPE_NAME:{C_NOT_NULL:0,C_TYPE_VALUE:"list",C_PK:0}
}

C_ENTITY_COLUMN_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_PK:{C_NOT_NULL:1,C_TYPE_VALUE:"int",C_PK:0},
    C_DESC:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_LINK_ENTITY:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0}
}

C_QUEUE_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_SOURCE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_SCHEMA:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_SOURCE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_INCREMENT:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0}
}

C_QUEUE_COLUMN_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
}

C_IDMAP_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_IDMAP_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_ATTRIBUTE_NK:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0}
}

C_IDMAP_COLUMN_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_IDMAP_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}

C_ANCHOR_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_ANCHOR_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
}

C_ANCHOR_COLUMN_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ANCHOR_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}

C_ATTRIBUTE_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_ATTRIBUTE_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}

C_ATTRIBUTE_COLUMN_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ATTRIBUTE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}

}

C_TIE_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_TIE_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_LINK_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0}
}

C_TIE_COLUMN_META_ATTRIBUTES = {
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_TIE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}

}

C_QUEUE_INCREMENT_META_ATTRIBUTES = {
    C_INCREMENT:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0}
}

C_QUEUE_ETL_META_ATTRIBUTES={
    C_QUEUE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}

C_IDMAP_ETL_META_ATTRIBUTES={
    C_IDMAP_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}

C_ANCHOR_ETL_META_ATTRIBUTES={
    C_ANCHOR_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}

C_ATTRIBUTE_ETL_META_ATTRIBUTES={
    C_ATTRIBUTE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}

C_TIE_ETL_META_ATTRIBUTES={
    C_TIE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_TABLE_TYPE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}

C_META_ATTRIBUTES = { # таблица метаданных и необходимые атрибуты
    C_SOURCE_META:C_SOURCE_META_ATTRIBUTES,
    C_ENTITY:C_ENTITY_META_ATTRIBUTES,
    C_ENTITY_COLUMN:C_ENTITY_COLUMN_META_ATTRIBUTES,
    C_QUEUE_TABLE_TYPE_NAME:C_QUEUE_META_ATTRIBUTES,
    C_QUEUE_COLUMN:C_QUEUE_COLUMN_META_ATTRIBUTES,
    C_IDMAP_TABLE_TYPE_NAME:C_IDMAP_META_ATTRIBUTES,
    C_IDMAP_COLUMN:C_IDMAP_COLUMN_META_ATTRIBUTES,
    C_QUEUE_TABLE_TYPE_NAME+"_"+C_INCREMENT:C_QUEUE_INCREMENT_META_ATTRIBUTES,
    C_ANCHOR_TABLE_TYPE_NAME:C_ANCHOR_META_ATTRIBUTES,
    C_ANCHOR_COLUMN:C_ANCHOR_COLUMN_META_ATTRIBUTES,
    C_ATTRIBUTE_TABLE_TYPE_NAME:C_ATTRIBUTE_META_ATTRIBUTES,
    C_ATTRIBUTE_COLUMN:C_ATTRIBUTE_COLUMN_META_ATTRIBUTES,
    C_TIE_TABLE_TYPE_NAME:C_TIE_META_ATTRIBUTES,
    C_TIE_COLUMN:C_TIE_COLUMN_META_ATTRIBUTES,
    C_QUEUE_ETL:C_QUEUE_ETL_META_ATTRIBUTES,
    C_IDMAP_ETL:C_IDMAP_ETL_META_ATTRIBUTES,
    C_ANCHOR_ETL:C_ANCHOR_ETL_META_ATTRIBUTES,
    C_ATTRIBUTE_ETL:C_ATTRIBUTE_ETL_META_ATTRIBUTES,
    C_TIE_ETL:C_TIE_ETL_META_ATTRIBUTES
}




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
C_DATETIME = "datetime"

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

C_TIMESTAMP_DBMS={ #тип данных даты и времени в разных СУБД
    C_MSSQL:C_DATETIME,
    C_POSTGRESQL:C_TIMESTAMP
}

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


C_ATTRIBUTE_TABLE_TYPE_LIST = [
    C_RK_TYPE_NAME,
    C_NK_TYPE_NAME,
    C_SOURCE_TYPE_NAME,
    C_ETL_TYPE_NAME,
    C_VALUE_TYPE_NAME,
    C_FROM_TYPE_NAME,
    C_TO_TYPE_NAME,
    C_QUEUE_ATTR_TYPE_NAME,
    C_UPDATE_TYPE_NAME,
    C_LINK_RK_TYPE_NAME,
    C_LINK_NK_TYPE_NAME
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
C_VALUE_DATA_SOURCE = "data source value" # данные с источника
C_LIST_OF_ATTRIBUTES = "list of attributes" # лист атрибутов
C_LIST_OF_ATTRIBUTES_VAR="&&list_of_attributes_id" # переменная в etl-шаблоне
C_VALUE_DATA_SOURCE_VAR="&&values_sql" # переменная в etl-шаблоне


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

C_TIE_ETL_TEMPLATE_VARIABLES={
    C_TABLE:{
        C_TIE_TABLE_TYPE_NAME:"&&tie_id",
        C_ETL_TYPE_NAME:"&&etl_id",
        C_RK_TYPE_NAME:"&&tie_rk_id",
        C_LINK_RK_TYPE_NAME:"&&link_anchor_rk_id",
        C_FROM_TYPE_NAME:"&&from_dttm_id",
        C_TO_TYPE_NAME:"&&to_dttm_id"
    },
    C_IDMAP_TABLE_TYPE_NAME:{
        C_IDMAP_TABLE_TYPE_NAME:"&&idmap_id",
        C_NK_TYPE_NAME:"&&idmap_nk_id",
        C_RK_TYPE_NAME:"&&idmap_rk_id"
    },
    C_LINK_IDMAP_TABLE_TYPE_NAME:{
        C_LINK_IDMAP_TABLE_TYPE_NAME:"&&link_idmap_id",
        C_NK_TYPE_NAME:"&&link_idmap_nk_id",
        C_RK_TYPE_NAME:"&&link_idmap_rk_id"
    },
    C_QUEUE_TABLE_TYPE_NAME:{
        C_QUEUE_TABLE_TYPE_NAME:"&&stg_table_id",
        C_NK_TYPE_NAME:C_CONCAT_NK_VAR,
        C_LINK_NK_TYPE_NAME:C_CONCAT_LINK_NK_VAR,
        C_UPDATE_TYPE_NAME:"&&update_timestamp_id",
        C_SOURCE:"&&source_id"
    },
    C_TEMP_TABLE:{
        1:"&&temp_rnum_table_id",
        2:"&&temp_change_table_id",
        3:"&&temp_insert_table_id"
    }
}

C_IDMAP_ETL_TEMPLATE_VARIABLES={
    C_TABLE:{
        C_IDMAP_TABLE_TYPE_NAME:"&&idmap_id",
        C_RK_TYPE_NAME:"&&idmap_rk_id",
        C_NK_TYPE_NAME:"&&idmap_nk_id",
        C_ETL_TYPE_NAME:"&&etl_id"
    },
    C_QUEUE_TABLE_TYPE_NAME:{
        C_QUEUE_TABLE_TYPE_NAME:"&&stg_table_id",
        C_NK_TYPE_NAME:"&&concat_nkey_sql",
        C_SOURCE:"&&source_id"
    },
    C_TEMP_TABLE:{
        1:"&&temp_concat_table_id",
        2:"&&temp_nkey_table_id"
    }
}

C_QUEUE_ETL_TEMPLATE_VARIABLES={
    C_TABLE:{
        C_QUEUE_TABLE_TYPE_NAME:"&&stg_table_id"
    },
    C_LIST_OF_ATTRIBUTES:C_LIST_OF_ATTRIBUTES_VAR,
    C_VALUE_DATA_SOURCE:C_VALUE_DATA_SOURCE_VAR
}

C_DATA_EXTRACT_TEMPLATE_VARIABLES={
    C_LIST_OF_ATTRIBUTES:"@@list_of_attributes",
    C_SCHEMA:"@@schema",
    C_TABLE:"@@table"
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
        C_TABLE_DDL_COMPONENTS:C_TABLE_TIE_TEMPLATE_VARIABLES_DICT,
        C_TABLE_ETL_COMPONENTS:C_TIE_ETL_TEMPLATE_VARIABLES
    },
    C_IDMAP_TABLE_TYPE_NAME:{
        C_TABLE_ATTRIBUTES:C_TABLE_IDMAP_ATTR_TYPE_LIST,
        C_TABLE_DDL_COMPONENTS:C_TABLE_IDMAP_TEMPLATE_VARIABLES_DICT,
        C_TABLE_ETL_COMPONENTS:C_IDMAP_ETL_TEMPLATE_VARIABLES
    },
    C_QUEUE_TABLE_TYPE_NAME:{
        C_TABLE_ATTRIBUTES:C_TABLE_QUEUE_ATTR_TYPE_LIST,
        C_TABLE_DDL_COMPONENTS:C_TABLE_QUEUE_TEMPLATE_VARIABLES_DICT,
        C_TABLE_ETL_COMPONENTS:C_QUEUE_ETL_TEMPLATE_VARIABLES
    }
}


def get_constant_list():
    """
    Возвращает список всех констант и их значения
    """
    return globals()
