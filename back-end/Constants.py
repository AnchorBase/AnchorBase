"""
Константы, используемые в программе
"""
#================================
#  Инструкция
#================================
# 1. НЕ ПЛОДИТЬ КОНСТАНТЫ (ПЕРЕД ТЕМ КАК ДОБАВИТЬ КОНСТАНТУ ТРЕБУЕТЯ ПРОВЕРИТЬ, ЧТО ЕЕ ЗНАЧЕНИЕ НЕ ПОВТОРЯЕТСЯ В ДРУГОЙ КОНСТАНТЕ)
# 2. ПО-ВОЗМОЖНОСТИ ДОБАВЛЯТЬ КОНСТАНТУ В СООТВЕТСТВУЮЩИЙ БЛОК (ВСЕ БЛОКИ ПРОДУБЛИРОВАНЫ В НАЧАЛЕ МОДУЛЯ)
# 3. ПРИ СОЗДАНИИ НОВОГО БЛОКА ПРОДУБЛИРОВАТЬ ЕГО В НАЧАЛЕ МОДУЛЯ
# 4. ЕСЛИ ЗНАЧЕНИЕ КОНСТАНТЫ МОЖНО СФОРМИРОВАТЬ ИЗ НЕСКОЛЬКИХ ЗНАЧЕНИЙ КОНСТАНТ - ЛУЧШЕ ТАК И СДЕЛАТЬ
# 5. НЕКОТОРЫЕ КОНСТАНТЫ НУЖНО ДОБАВЛЯТЬ ДОБАВЛЯТЬ В СПИСКИ (УКАЗАНО В КОММЕНТАРИЯХ) - БЫТЬ ВНИМАТЕЛЬНЫМ


#================================
#  ЛОГИЧЕСКИЕ БЛОКИ
#================================
# - Основные понятия
# - Параметры подключения к СУБД
# - SQL
# - ETL
# - Таблицы ХД
# - Схемы ХД
# - Атрибуты таблиц ХД
# - Настройки системы
# - Метаданные
# - СУБД
# - Типы данных
# - Цвета шрифтов для консоли


#================================
#  Основные понятия
#================================
C_ENTITY = "entity" # сущность
C_LINK_ENTITY = "link_entity"  # связанная сущность
C_ENTITY_ATTRIBUTE = "entity_attribute" # атрибут сущности
C_TABLE = "table" # таблица
C_COLUMN = "column" # колонка
C_ATTRIBUTE = "attribute" # атрибут
C_SCHEMA = "schema" # схема
C_NAME = "name" # наименование
C_SOURCE = "source" # источник
C_DDL = "ddl"
C_VIEW = "view"
C_ETL = "etl"
C_DATATYPE = "datatype" # тип данных
C_LENGTH = "length" # длина значения атрибута
C_SCALE = "scale" # количество знаков после запятой
C_DWH = "dwh" # ХД
C_INCREMENT = "increment" # инкремент
C_PK = "pk" # ключ
C_DESC="description" # описание

#================================
#  Параметры подключения к СУБД
#================================
C_SERVER = "server" # хост
C_DATABASE = "database" # наименование базы данных
C_USER = "user" # пользователь
C_PASSWORD = "password" # пароль
C_PORT = "port" # порт

#================================
#  SQL
#================================
C_CAST = "CAST" # операция CAST
C_CONCAT_SYMBOL = "@@" #символ для конкатенаци


#================================
#  ETL
#================================
C_STATUS="status"
C_STATUS_IN_PROGRESS="in progress"
C_STATUS_FAIL="fail"
C_STATUS_SUCCESS="success"
C_STATUS_PARTLY_SUCCESS="partly success"
C_STATUS_START="start"
C_START_DATETIME="start_datetime"
C_END_DATETIME="end_datetime"
C_PACKAGE="package"
C_ERROR="error"
#================================
#  Таблицы ХД
#================================
#!!! При создании нового типа таблицы добавить константу в список C_TABLE_TYPE_LIST
C_ANCHOR = "anchor" # якорная таблица
C_ATTRIBUTE_TABLE = "attribute" # таблица атрибут
C_TIE = "tie" # таблица связи
C_IDMAP = "idmap" # таблица idmap
C_QUEUE = "queue" # таблицы очереди (queue)
C_TABLE_TYPE_LIST = [ # список типов таблиц (для проверки корректности указанного типа)
    C_ANCHOR,
    C_ATTRIBUTE_TABLE,
    C_TIE,
    C_QUEUE,
    C_IDMAP
]
C_TABLE_NAME_POSTFIX={ # постфиксы таблиц
    C_ANCHOR: "_an",
    C_ATTRIBUTE_TABLE: "_attr",
    C_TIE: "_tie",
    C_IDMAP: "_idmap",
    C_QUEUE: "_queue"
}
#================================
#  Схемы ХД
#================================
C_STG_SCHEMA="stg"
C_IDMAP_SCHEMA="idmap"
C_AM_SCHEMA="am"
C_WRK_SCHEMA="wrk"
C_SCHEMA_TABLE_TYPE = { # наименование схемы в соответствии с типом таблицы
    C_QUEUE:C_STG_SCHEMA,
    C_IDMAP:C_IDMAP_SCHEMA,
    C_ANCHOR:C_AM_SCHEMA,
    C_ATTRIBUTE_TABLE:C_AM_SCHEMA,
    C_TIE:C_AM_SCHEMA
}
#================================
#  Атрибуты таблиц ХД
#================================
# ТИПЫ АТРИБУТОВ
#!!! При создании нового атрибута добавить константу в список C_ATTRIBUTE_TABLE_TYPE_LIST!!!
C_RK = "rk" # суррогатный ключ
C_NK = "nk" # натуральный ключ
C_SOURCE_ATTR = C_SOURCE # источник
C_ETL_ATTR = C_ETL # param: идентификатор процесса загрузки данных
C_VALUE = "value" # атрибут, хранящий значение в таблице attribute
C_FROM = "from" # дата начала действия записи
C_TO = "to" # дата окончания действия записи
C_LINK_RK = "link_rk" # суррогатный ключ связанной сущности в таблице tie
C_QUEUE_ATTR = "queue_attr" # атрибут таблицы queue из источника
C_UPDATE = "update" # атрибут, хранящий инкремент, таблицы источника
C_ATTRIBUTE_TABLE_TYPE_LIST = [ # cписок типов атрибутов (для проверки корректности указанного типа)
    C_RK,
    C_NK,
    C_SOURCE_ATTR,
    C_ETL_ATTR,
    C_VALUE,
    C_FROM,
    C_TO,
    C_QUEUE_ATTR,
    C_UPDATE,
    C_LINK_RK
]
# НАИМЕНОВАНИЯ АТРИБУТОВ В ХД
C_SOURCE_ATTRIBUTE_NAME="source_system_id"
C_ETL_ATTRIBUTE_NAME="etl_id"
C_FROM_ATTRIBUTE_NAME="from_dttm"
C_TO_ATTRIBUTE_NAME="to_dttm"
C_UPDATE_TIMESTAMP_NAME = "update_timestamp"
#================================
#  Настройки системы
#================================
C_CONFIG_FILE_PATH = "dwh_config.py" # путь до файла с конфигами подключения к ХД
C_MSSQL_DRIVER_MACOS_PATH = "/usr/local/lib/libtdsodbc.so" # расположение драйвера в MacOS
C_TDS_VERSION = '7.3' # версия TDS для pyodbc
#================================
#  Метаданные
#================================
# !!! При добавлении новой переменной метаданных, добавить ее в список C_META_TABLES
C_SOURCE_META = C_SOURCE # таблица с параметрами источников
C_QUEUE_COLUMN = C_QUEUE + "_" + C_COLUMN # атрибут таблицы очереди (queue)
C_IDMAP_COLUMN = C_IDMAP + "_" + C_COLUMN # атрибуты idmap
C_ANCHOR_COLUMN = C_ANCHOR + "_" + C_COLUMN # атрибуты якорной таблицы
C_ATTRIBUTE_COLUMN = C_ATTRIBUTE_TABLE + "_" + C_COLUMN # атрибуты таблицы атрибут
C_TIE_COLUMN = C_TIE + "_" + C_COLUMN # атрибуты таблицы связи
C_ENTITY_COLUMN = C_ENTITY+"_"+C_COLUMN # атрибуты сущности
C_QUEUE_ETL="queue_etl" # логи etl таблицы очереди (queue)
C_IDMAP_ETL="idmap_etl" # логи etl таблицы idmap
C_ANCHOR_ETL="anchor_etl" # логи etl таблицы якоря
C_ATTRIBUTE_ETL="attribute_etl" # логи etl таблицы атрибут
C_TIE_ETL="tie_etl" # логи etl таблицы связи
C_QUEUE_INCREMENT = C_QUEUE+"_"+C_INCREMENT
C_META_TABLES = [ # список таблиц метаданных (для проверки корректности)
    C_SOURCE_META,
    C_ENTITY,
    C_ENTITY_COLUMN,
    C_QUEUE,
    C_QUEUE_COLUMN,
    C_IDMAP,
    C_IDMAP_COLUMN,
    C_ANCHOR,
    C_ANCHOR_COLUMN,
    C_ATTRIBUTE_TABLE,
    C_ATTRIBUTE_COLUMN,
    C_TIE,
    C_TIE_COLUMN,
    C_QUEUE_COLUMN,
    C_ETL,
    C_QUEUE_ETL,
    C_IDMAP_ETL,
    C_ANCHOR_ETL,
    C_ATTRIBUTE_ETL,
    C_TIE_ETL,
    C_QUEUE_INCREMENT
]
# АТРИБУТЫ МЕТАДАННЫХ
C_SOURCE_NAME = C_SOURCE+"_"+C_NAME # наименование таблицы на источнике
C_SOURCE_ID="source_id"
C_ATTRIBUTE_NK = "column_nk"
C_LINK_ATTRIBUTE_NK = "link_column_nk"
# СВОЙСТВА МЕТАДАННЫХ
C_NOT_NULL = "not null" # признак обязательного атрибута
C_TYPE_VALUE = "type" # тип атрибута (строка, лист)
# НЕОБХОДИМЫЕ АТРИБУТЫ МЕТАДАННЫХ ДЛЯ КАЖДОЙ ТАБЛИЦЫ МЕТАДАННЫХ
# !!! Добавить необходимые атрибуты в список C_META_ATTRIBUTES!!!
C_SOURCE_META_ATTRIBUTES = { # необходимые атрибуты источника
    C_SERVER:{C_NOT_NULL:1,C_TYPE_VALUE:"str", C_PK:0},
    C_DATABASE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_USER:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_PASSWORD:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_PORT:{C_NOT_NULL:1,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_DESC:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_SOURCE_ID:{C_NOT_NULL:1,C_TYPE_VALUE:"int",C_PK:1}
}
C_ENTITY_META_ATTRIBUTES = { # необходимые атрибуты сущности
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_DESC:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY+"_"+C_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_SOURCE_META:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_QUEUE:{C_NOT_NULL:1, C_TYPE_VALUE: "list", C_PK:0},
    C_IDMAP:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_ANCHOR:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_ATTRIBUTE_TABLE:{C_NOT_NULL:1, C_TYPE_VALUE: "list", C_PK:0},
    C_TIE:{C_NOT_NULL:0, C_TYPE_VALUE: "list", C_PK:0}
}
C_ENTITY_COLUMN_META_ATTRIBUTES = { # необходимые атрибуты атрибутов сущности
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
C_QUEUE_META_ATTRIBUTES = { # необходимые атрибуты таблицы очереди
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_SOURCE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_SCHEMA:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_SOURCE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_INCREMENT:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0}
}
C_QUEUE_COLUMN_META_ATTRIBUTES = { # необходимые атрибуты атрибутов таблицы сущности
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
}
C_IDMAP_META_ATTRIBUTES = { # необходимые атрибуты idmap
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_IDMAP_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_ATTRIBUTE_NK:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0}
}
C_IDMAP_COLUMN_META_ATTRIBUTES = { # необходимые атрибуты атрибутов idmap
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_IDMAP:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}
C_ANCHOR_META_ATTRIBUTES = { # необходимые атрибуты якоря
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_ANCHOR_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
}
C_ANCHOR_COLUMN_META_ATTRIBUTES = { # необходимые атрибуты атрибутов якоря
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ANCHOR:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}
C_ATTRIBUTE_META_ATTRIBUTES = { # необходимые атрибуты таблицы атрибут
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_ATTRIBUTE_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}
C_ATTRIBUTE_COLUMN_META_ATTRIBUTES = { # необходимые атрибуты атрибутов таблицы атрибут
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ATTRIBUTE_TABLE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}

}
C_TIE_META_ATTRIBUTES = { # необходимые атрибуты таблицы связи
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:1},
    C_TIE_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"list",C_PK:0},
    C_LINK_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE:{C_NOT_NULL:1, C_TYPE_VALUE: "list", C_PK:0}
}
C_TIE_COLUMN_META_ATTRIBUTES = { # необходимые атрибуты атрибутов таблицы связи
    C_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_TYPE_VALUE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_TIE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0}

}
C_QUEUE_INCREMENT_META_ATTRIBUTES = { # необходимые атрибуты таблицы с инкрементом таблицы очереди
    C_INCREMENT:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0}
}
C_QUEUE_ETL_META_ATTRIBUTES={ # необходимые атрибуты логов таблицы очереди
    C_QUEUE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}
C_IDMAP_ETL_META_ATTRIBUTES={ # необходимые атрибуты логов idmap
    C_IDMAP:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_QUEUE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}
C_ANCHOR_ETL_META_ATTRIBUTES={ # необходимые атрибуты логов якоря
    C_ANCHOR:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}
C_ATTRIBUTE_ETL_META_ATTRIBUTES={ # необходимые атрибуты логов таблицы атрибут
    C_ATTRIBUTE_TABLE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_QUEUE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}
C_TIE_ETL_META_ATTRIBUTES={ # необходимые атрибуты логов таблицы связи
    C_TIE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_QUEUE:{C_NOT_NULL:1, C_TYPE_VALUE: "str", C_PK:0},
    C_STATUS:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ERROR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ETL:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0}
}
C_ETL_META_ATTRIBUTES={ # необходимые атрибуты логов джобов
    C_ETL_ATTRIBUTE_NAME:{C_NOT_NULL:1,C_TYPE_VALUE:"int",C_PK:1},
    C_START_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_END_DATETIME:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY_COLUMN:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_ETL:{C_NOT_NULL:0,C_TYPE_VALUE:"list",C_PK:0},
    C_IDMAP_ETL:{C_NOT_NULL:0,C_TYPE_VALUE:"list",C_PK:0},
    C_ANCHOR_ETL:{C_NOT_NULL:0,C_TYPE_VALUE:"list",C_PK:0},
    C_ATTRIBUTE_ETL:{C_NOT_NULL:0,C_TYPE_VALUE:"list",C_PK:0},
    C_TIE_ETL:{C_NOT_NULL:0,C_TYPE_VALUE:"list",C_PK:0}
}
C_META_ATTRIBUTES = { # таблица метаданных и необходимые атрибуты
    C_SOURCE_META:C_SOURCE_META_ATTRIBUTES,
    C_ENTITY:C_ENTITY_META_ATTRIBUTES,
    C_ENTITY_COLUMN:C_ENTITY_COLUMN_META_ATTRIBUTES,
    C_QUEUE:C_QUEUE_META_ATTRIBUTES,
    C_QUEUE_COLUMN:C_QUEUE_COLUMN_META_ATTRIBUTES,
    C_IDMAP:C_IDMAP_META_ATTRIBUTES,
    C_IDMAP_COLUMN:C_IDMAP_COLUMN_META_ATTRIBUTES,
    C_QUEUE_INCREMENT:C_QUEUE_INCREMENT_META_ATTRIBUTES,
    C_ANCHOR:C_ANCHOR_META_ATTRIBUTES,
    C_ANCHOR_COLUMN:C_ANCHOR_COLUMN_META_ATTRIBUTES,
    C_ATTRIBUTE_TABLE:C_ATTRIBUTE_META_ATTRIBUTES,
    C_ATTRIBUTE_COLUMN:C_ATTRIBUTE_COLUMN_META_ATTRIBUTES,
    C_TIE:C_TIE_META_ATTRIBUTES,
    C_TIE_COLUMN:C_TIE_COLUMN_META_ATTRIBUTES,
    C_QUEUE_ETL:C_QUEUE_ETL_META_ATTRIBUTES,
    C_IDMAP_ETL:C_IDMAP_ETL_META_ATTRIBUTES,
    C_ANCHOR_ETL:C_ANCHOR_ETL_META_ATTRIBUTES,
    C_ATTRIBUTE_ETL:C_ATTRIBUTE_ETL_META_ATTRIBUTES,
    C_TIE_ETL:C_TIE_ETL_META_ATTRIBUTES,
    C_ETL:C_ETL_META_ATTRIBUTES
}
#================================
#  СУБД
#================================
C_MSSQL = "mssql"  # MSSQL
C_POSTGRESQL = "postgresql" # PostgreSQL
C_AVAILABLE_SOURCE_LIST = [C_MSSQL] # фиксированный список СУБД, с которым AnchorBase умеет работать как с источником
C_AVAILABLE_DWH_LIST = [C_POSTGRESQL] # фиксированный список СУБД, с которым AnchorBase умеет работать как с DWH
C_CNCT_PARAMS = [  # фиксированный список параметров подключения
    C_SERVER,
    C_DATABASE,
    C_USER,
    C_PASSWORD,
    C_PORT
]
#TODO устаревшая константа. Убрать, когда будет убран класс DBMS
C_DBMS_PURPOSE_LIST = [C_SOURCE,C_DWH] # фиксированный список назначений СУБД (источник, ХД)


#================================
#  Типы данных
#================================
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
C_UUID = "uuid"
C_JSON = "json"
# СПИСКИ ТИПОВ ДАННЫХ ДЛЯ КАЖДОЙ СУБД
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
# компоненты СУБД: типы данных и т.д.
C_DBMS_COMPONENTS = {
    C_POSTGRESQL:{
        C_DATATYPE:C_POSTGRESQL_DATA_TYPE_LIST
    }
}
#================================
#  Цвета шрифтов для консоли
#================================
C_COLOR_HEADER = '\033[95m' # заголовок
C_COLOR_OKBLUE = '\033[94m' # синий
C_COLOR_OKCYAN = '\033[96m' # фиолетовый
C_COLOR_OKGREEN = '\033[92m' # зеленый
C_COLOR_WARNING = '\033[93m' # желтый
C_COLOR_FAIL = '\033[91m' # красный
C_COLOR_ENDC = '\033[0m' # дефолтный цвет
C_COLOR_BOLD = '\033[1m' # жирный шрифт
C_COLOR_UNDERLINE = '\033[4m' #  подчеркивание
