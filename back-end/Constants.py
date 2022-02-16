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
# - Команды консоли


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
C_SOURCE_COLUMN = "source_column" # атрибут источник
C_SOURCE_TABLE = "source_table" # таблица источник
C_DDL = "ddl"
C_VIEW = "view"
C_ETL = "etl"
C_DATATYPE = "datatype" # тип данных
C_LENGTH = "length" # длина значения атрибута
C_SCALE = "scale" # количество знаков после запятой
C_DWH = "dwh" # ХД
C_INCREMENT = "increment" # инкремент
C_PK = "pk" # ключ
C_FK="fk" # внешний ключ
C_DESC="description" # описание
C_ID="id" # идентификатор
C_DATA="data" # данные
C_MESSAGE="message" # сообщение
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
C_DUMMY_UUID="00000000-0000-0000-0000-000000000000" # пустой uuid
#================================
#  ETL
#================================
C_STATUS="status"
C_STATUS_IN_PROGRESS="in progress"
C_STATUS_FAIL="fail"
C_STATUS_SUCCESS="success    " # пробелы поставлены для корректного возврата каретки в консоли
C_STATUS_PARTLY_SUCCESS="partly success"
C_STATUS_START="start"
C_START_DATETIME="start_datetime"
C_END_DATETIME="end_datetime"
C_PACKAGE="package"
C_ERROR="error"
C_DURATION="duration" # длительность отработки etl
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
C_AM_SCHEMA="dds"
C_WRK_SCHEMA="wrk"
C_META_SCHEMA="abase_meta"
C_SCHEMA_LIST=[
    C_STG_SCHEMA,
    C_IDMAP_SCHEMA,
    C_AM_SCHEMA,
    C_WRK_SCHEMA
]
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
C_META_CONFIG="metadata_config.py" # наименование файла с параметрами подключения к метеданным
C_DBMS_TYPE="dbms_type" # тип СУБД
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
C_RK_DESC="Суррогат сущности" # описание суррогата для метаданных
C_LINK_RK_DESC="Суррогат связанной сущности" # описание суррогата связанной сущности для метаданных
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
    C_PK:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_RK:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_FK:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_DESC:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ENTITY:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_DATATYPE:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
    C_LENGTH:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_SCALE:{C_NOT_NULL:0,C_TYPE_VALUE:"int",C_PK:0},
    C_LINK_ENTITY:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_QUEUE_COLUMN:{C_NOT_NULL:0,C_TYPE_VALUE:"list",C_PK:0},
    C_ANCHOR:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_ATTRIBUTE_TABLE:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0},
    C_TIE:{C_NOT_NULL:0,C_TYPE_VALUE:"str",C_PK:0}
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
    C_ENTITY_COLUMN:{C_NOT_NULL:1,C_TYPE_VALUE:"str",C_PK:0},
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
C_AVAILABLE_SOURCE_LIST = [C_MSSQL, C_POSTGRESQL] # фиксированный список СУБД, с которым AnchorBase умеет работать как с источником
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
#================================
#  Команды консоли
#================================
# не забыть добавить новые команды в список C_CONSOLE_COMMAND_LIST
C_GET_SOURCE="get_source" # получение источника/источников
C_ADD_SOURCE="add_source" # добавление нового источника
C_ALTER_SOURCE="alter_source" # изменение параметров источника
C_GET_SOURCE_TYPE="get_source_type" # показывается все типы источников, с которыми AnchorBase умеет работать
C_GET_ENTITY="get_entity" # получение сущности
C_GET_ENTITY_ATTR="get_attr" # получение атрибута сущности
C_GET_ENTITY_SOURCE="get_entity_source" # получение источников сущности
C_GET_ATTR_SOURCE="get_attr_source" # получение источников атрибутов сущности
C_START_JOB="load_data" # загрузка данных в ХД
C_GET_LAST_ETL="get_last_etl" # получение информации о последнем etl
C_GET_ETL_HIST="get_etl_hist" # получение логов etl-процессов
C_GET_ETL_DETAIL="get_etl_detail" # получение детализации по etl-процессу
C_ADD_ENTITY="create_entity" # добавление сущности в ХД
C_ALTER_ENTITY="alter_entity" # изменение сущности в ХД
C_DROP_ENTITY="drop_entity" # удаляет указанную сущность
C_GET_META_CONFIG="get_meta_config" # возвращает параметры подключения к метаданным
C_UPDATE_META_CONFIG="update_meta_config" # изменяет параметры подключения к метаданным
C_CREATE_META="install_meta" # создает схему и таблицы метаданных
C_GET_DWH_CONFIG="get_dwh_config" # возвращает параметры подключения к ХД
C_UPDATE_DWH_CONFIG="update_dwh_config" # изменяет параметры подключения к ХД
C_CREATE_DWH="install_dwh" # создает схемы для ХД
C_EXIT="exit"
C_HELP="help"
C_CONSOLE_COMMAND_LIST=[
    C_GET_SOURCE,
    C_ADD_SOURCE,
    C_ALTER_SOURCE,
    C_GET_SOURCE_TYPE,
    C_GET_ENTITY,
    C_GET_ENTITY_ATTR,
    C_GET_ATTR_SOURCE,
    C_GET_ENTITY_SOURCE,
    C_START_JOB,
    C_GET_LAST_ETL,
    C_GET_ETL_HIST,
    C_GET_ETL_DETAIL,
    C_ADD_ENTITY,
    C_ALTER_ENTITY,
    C_DROP_ENTITY,
    C_GET_META_CONFIG,
    C_UPDATE_META_CONFIG,
    C_CREATE_META,
    C_GET_DWH_CONFIG,
    C_UPDATE_DWH_CONFIG,
    C_CREATE_DWH,
    C_EXIT,
    C_HELP
]
# АРГУМЕНТЫ КОМАНД КОНСОЛИ
C_NAME_CONSOLE_ARG="-name"
C_ID_CONSOLE_ARG="-id"
C_SERVER_CONSOLE_ARG="-server"
C_DATABASE_CONSOLE_ARG="-db"
C_USER_CONSOLE_ARG="-user"
C_PASSWORD_CONSOLE_ARG="-pwrd"
C_PORT_CONSOLE_ARG="-port"
C_DESC_CONSOLE_ARG="-desc"
C_TYPE_CONSOLE_ARG="-type"
C_ENTITY_CONSOLE_ARG="-entity"
C_ENTITY_ATTR_CONSOLE_ARG="-attr"
C_SOURCE_ID_CONSOLE_ARG="-source_id"
C_DATE_CONSOLE_ARG="-date"
C_ETL_ID_CONSOLE_ARG="-etl_id"
C_FILE_CONSOLE_ARG="-file"
C_CONSOLE_ARGS={
    C_GET_SOURCE:{
        C_NAME_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование источника (необязательный)"},
        C_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id источника (необязательный)"},
    },
    C_ADD_SOURCE:{
        C_NAME_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"наименование источника (обязательный)"},
        C_DESC_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"описание источника (необязательный)"},
        C_SERVER_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"сервер/хост (обязательный)"},
        C_DATABASE_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"база данных (обязательный)"},
        C_USER_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"логин (обязательный)"},
        C_PASSWORD_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"пароль (обязательный)"},
        C_PORT_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"порт (обязательный)"},
        C_TYPE_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"тип источника (обязательный)"}
    },
    C_ALTER_SOURCE:{
        C_ID_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"id источника (обязательный)"},
        C_NAME_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"новое наименование источника (необязательный)"},
        C_DESC_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"новое описание источника (необязательный)"},
        C_SERVER_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"новый сервер/хост (необязательный)"},
        C_DATABASE_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"новая база данных (необязательный)"},
        C_USER_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"новый логин (необязательный)"},
        C_PASSWORD_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"новый пароль (необязательный)"},
        C_PORT_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"новый порт (необязательный)"},
        C_TYPE_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"новый тип источника (необязательный)"}
    },
    C_UPDATE_META_CONFIG:{
        C_SERVER_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"сервер/хост (обязательный)"},
        C_DATABASE_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"база данных (обязательный)"},
        C_USER_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"логин (обязательный)"},
        C_PASSWORD_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"пароль (обязательный)"},
        C_PORT_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"порт (обязательный)"}
    },
    C_UPDATE_DWH_CONFIG:{
        C_SERVER_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"сервер/хост (обязательный)"},
        C_DATABASE_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"база данных (обязательный)"},
        C_USER_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"логин (обязательный)"},
        C_PASSWORD_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"пароль (обязательный)"},
        C_PORT_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"порт (обязательный)"}
    },
    C_GET_ENTITY:{
        C_NAME_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование сущности (необязательный)"},
        C_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id сущности (необязательный)"},
    },
    C_GET_ENTITY_ATTR:{
        C_NAME_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование атрибута (необязательный)"},
        C_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id атрибута (необязательный)"},
        C_ENTITY_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование сущности (необязательный)"}
    },
    C_GET_ATTR_SOURCE:{
        C_NAME_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование атрибута (необязательный)"},
        C_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id атрибута (необязательный)"},
        C_ENTITY_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование сущности (необязательный)"},
        C_SOURCE_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id источника (необязательный)"}
    },
    C_GET_ENTITY_SOURCE:{
        C_NAME_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование сущности (необязательный)"},
        C_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id сущности (необязательный)"},
        C_SOURCE_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id источника (необязательный)"}
    },
    C_ADD_ENTITY:{
        C_FILE_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"путь до файла с параметрами сущности в формате json (необязательный)"}
    },
    C_START_JOB:{
        C_ENTITY_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id сущности, данные которой требуется обновить (необязательный)"},
        C_ENTITY_ATTR_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"id атрибута, данные которого требуется обновить (необязательный)"}
    },
    C_GET_ETL_HIST:{
        C_DATE_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"дата выполнения etl-процесса в формате YYYY-MM-DD (необязательный)"}
    },
    C_GET_ETL_DETAIL:{
        C_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"дата выполнения etl-процесса в формате YYYY-MM-DD (необязательный)"},
        C_ETL_ID_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"дата выполнения etl-процесса в формате YYYY-MM-DD (необязательный)"}
    },
    C_ALTER_ENTITY:{
        C_ID_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"id сущности (обязательный)"},
        C_NAME_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование сущности (необязательный)"},
        C_DESC_CONSOLE_ARG:{C_NOT_NULL:0,C_DESC:"наименование сущности (необязательный)"}
    },
    C_DROP_ENTITY:{
        C_ID_CONSOLE_ARG:{C_NOT_NULL:1,C_DESC:"id сущности (обязательный)"}
    }
}
# описание команд консоли
C_CONSOLE_COMMAND_DESC={
    C_GET_META_CONFIG:"\n"+C_COLOR_HEADER+C_GET_META_CONFIG+C_COLOR_ENDC+"\n"+
                       C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВозвращает параметры подключения к метаданным ХД",
    C_UPDATE_META_CONFIG:"\n"+C_COLOR_HEADER+C_UPDATE_META_CONFIG+C_COLOR_ENDC+"\n"+
                         C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tИзменяет параметры подключения к метаданным\n"+
                         C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                         C_COLOR_OKCYAN+C_SERVER_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_META_CONFIG).get(C_SERVER_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                         C_COLOR_OKCYAN+C_DATABASE_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_META_CONFIG).get(C_DATABASE_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                         C_COLOR_OKCYAN+C_USER_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_META_CONFIG).get(C_USER_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                         C_COLOR_OKCYAN+C_PASSWORD_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_META_CONFIG).get(C_PASSWORD_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                         C_COLOR_OKCYAN+C_PORT_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_META_CONFIG).get(C_PORT_CONSOLE_ARG).get(C_DESC),
    C_CREATE_META:"\n"+C_COLOR_HEADER+C_CREATE_META+C_COLOR_ENDC+"\n"+
                  C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tСоздает таблицы метаданных\n",
    C_GET_DWH_CONFIG:"\n"+C_COLOR_HEADER+C_GET_DWH_CONFIG+C_COLOR_ENDC+"\n"+
                      C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВозвращает параметры подключения к ХД",
    C_UPDATE_DWH_CONFIG:"\n"+C_COLOR_HEADER+C_UPDATE_DWH_CONFIG+C_COLOR_ENDC+"\n"+
                         C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tИзменяет параметры подключения к ХД\n"+
                         C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                         C_COLOR_OKCYAN+C_SERVER_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_DWH_CONFIG).get(C_SERVER_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                         C_COLOR_OKCYAN+C_DATABASE_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_DWH_CONFIG).get(C_DATABASE_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                         C_COLOR_OKCYAN+C_USER_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_DWH_CONFIG).get(C_USER_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                         C_COLOR_OKCYAN+C_PASSWORD_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_DWH_CONFIG).get(C_PASSWORD_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                         C_COLOR_OKCYAN+C_PORT_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_UPDATE_DWH_CONFIG).get(C_PORT_CONSOLE_ARG).get(C_DESC),
    C_CREATE_DWH:"\n"+C_COLOR_HEADER+C_CREATE_DWH+C_COLOR_ENDC+"\n"+
                  C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tСоздает таблицы ХД\n",
    C_GET_SOURCE:"\n"+C_COLOR_HEADER+C_GET_SOURCE+C_COLOR_ENDC+"\n"+
                 C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВозвращает источники и их свойства\n"+
                 C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                 C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_SOURCE).get(C_ID_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_NAME_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_SOURCE).get(C_NAME_CONSOLE_ARG).get(C_DESC),
    C_ADD_SOURCE:"\n"+C_COLOR_HEADER+C_ADD_SOURCE+C_COLOR_ENDC+"\n"+
                 C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tДобавляет новый источник\n"+
                 C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                 C_COLOR_OKCYAN+C_NAME_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_SOURCE).get(C_NAME_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_DESC_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_SOURCE).get(C_DESC_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_SERVER_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_SOURCE).get(C_SERVER_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_DATABASE_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_SOURCE).get(C_DATABASE_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_USER_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_SOURCE).get(C_USER_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_PASSWORD_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_SOURCE).get(C_PASSWORD_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_PORT_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_SOURCE).get(C_PORT_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_TYPE_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_SOURCE).get(C_TYPE_CONSOLE_ARG).get(C_DESC)+"\n\t",
    C_ALTER_SOURCE:"\n"+C_COLOR_HEADER+C_ALTER_SOURCE+C_COLOR_ENDC+"\n"+
                   C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tДобавляет новый источник\n"+
                   C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                   C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_SOURCE).get(C_ID_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                   C_COLOR_OKCYAN+C_NAME_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_SOURCE).get(C_NAME_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                   C_COLOR_OKCYAN+C_DESC_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_SOURCE).get(C_DESC_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                   C_COLOR_OKCYAN+C_SERVER_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_SOURCE).get(C_SERVER_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                   C_COLOR_OKCYAN+C_DATABASE_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_SOURCE).get(C_DATABASE_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                   C_COLOR_OKCYAN+C_USER_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_SOURCE).get(C_USER_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                   C_COLOR_OKCYAN+C_PASSWORD_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_SOURCE).get(C_PASSWORD_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                   C_COLOR_OKCYAN+C_PORT_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_SOURCE).get(C_PORT_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                   C_COLOR_OKCYAN+C_TYPE_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_SOURCE).get(C_TYPE_CONSOLE_ARG).get(C_DESC)+"\n\t",
    C_GET_SOURCE_TYPE:"\n"+C_COLOR_HEADER+C_GET_SOURCE_TYPE+C_COLOR_ENDC+"\n"+
                      C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tПоказывает все типы источников, с которыми AnchorBase умеет работать",
    C_GET_ENTITY:"\n"+C_COLOR_HEADER+C_GET_ENTITY+C_COLOR_ENDC+"\n"+
                 C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВозвращает сущности и их свойства\n"+
                 C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                 C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ENTITY).get(C_ID_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                 C_COLOR_OKCYAN+C_NAME_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ENTITY).get(C_NAME_CONSOLE_ARG).get(C_DESC),
    C_GET_ENTITY_ATTR:"\n"+C_COLOR_HEADER+C_GET_ENTITY_ATTR+C_COLOR_ENDC+"\n"+
                     C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВозвращает атрибуты и их свойства\n"+
                     C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                     C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ENTITY_ATTR).get(C_ID_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                     C_COLOR_OKCYAN+C_NAME_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ENTITY_ATTR).get(C_NAME_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                     C_COLOR_OKCYAN+C_ENTITY_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ENTITY_ATTR).get(C_ENTITY_CONSOLE_ARG).get(C_DESC),
    C_GET_ATTR_SOURCE:"\n"+C_COLOR_HEADER+C_GET_ATTR_SOURCE+C_COLOR_ENDC+"\n"+
                      C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВозвращает атрибуты и их источники\n"+
                      C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                      C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ATTR_SOURCE).get(C_ID_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                      C_COLOR_OKCYAN+C_NAME_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ATTR_SOURCE).get(C_NAME_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                      C_COLOR_OKCYAN+C_ENTITY_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ATTR_SOURCE).get(C_ENTITY_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                      C_COLOR_OKCYAN+C_SOURCE_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ATTR_SOURCE).get(C_SOURCE_ID_CONSOLE_ARG).get(C_DESC),
    C_GET_ENTITY_SOURCE:"\n"+C_COLOR_HEADER+C_GET_ENTITY_SOURCE+C_COLOR_ENDC+"\n"+
                        C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВозвращает сущности и их источники\n"+
                        C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                        C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ENTITY_SOURCE).get(C_ID_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                        C_COLOR_OKCYAN+C_NAME_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ENTITY_SOURCE).get(C_NAME_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                        C_COLOR_OKCYAN+C_SOURCE_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ENTITY_SOURCE).get(C_SOURCE_ID_CONSOLE_ARG).get(C_DESC),
    C_ADD_ENTITY:"\n"+C_COLOR_HEADER+C_ADD_ENTITY+C_COLOR_ENDC+"\n"+
                        C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tДобавляет сущность в ХД на основе заданных параметров. Генерирует таблицы и ETL.\n"+
                        C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                        C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ADD_ENTITY).get(C_FILE_CONSOLE_ARG).get(C_DESC),
    C_ALTER_ENTITY:"\n"+C_COLOR_HEADER+C_ALTER_ENTITY+C_COLOR_ENDC+"\n"+
                    C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tИзменяет созданную ранее сущность.\n"+
                    C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                    C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_ENTITY).get(C_ID_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                    C_COLOR_OKCYAN+C_NAME_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_ENTITY).get(C_NAME_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                    C_COLOR_OKCYAN+C_DESC_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_ALTER_ENTITY).get(C_DESC_CONSOLE_ARG).get(C_DESC),
    C_DROP_ENTITY:"\n"+C_COLOR_HEADER+C_DROP_ENTITY+C_COLOR_ENDC+"\n"+
                   C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tУдаляет сущность.\n"+
                   C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                   C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_DROP_ENTITY).get(C_ID_CONSOLE_ARG).get(C_DESC),
    C_START_JOB:"\n"+C_COLOR_HEADER+C_START_JOB+C_COLOR_ENDC+"\n"+
                C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tЗапускает загрузку данных в ХД\n"+
                C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                C_COLOR_OKCYAN+C_ENTITY_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_START_JOB).get(C_ENTITY_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                C_COLOR_OKCYAN+C_ENTITY_ATTR_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_START_JOB).get(C_ENTITY_ATTR_CONSOLE_ARG).get(C_DESC),
    C_GET_LAST_ETL:"\n"+C_COLOR_HEADER+C_GET_LAST_ETL+C_COLOR_ENDC+"\n"+
                   C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВыдает информацию о последнем ETL",
    C_GET_ETL_HIST:"\n"+C_COLOR_HEADER+C_GET_ETL_HIST+C_COLOR_ENDC+"\n"+
                  C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВыдает логи по ETL-процессу\n"+
                  C_COLOR_BOLD+"Аргументы:"+C_COLOR_ENDC+"\n\t"+
                  C_COLOR_OKCYAN+C_DATE_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ETL_HIST).get(C_DATE_CONSOLE_ARG).get(C_DESC),
    C_GET_ETL_DETAIL:"\n"+C_COLOR_HEADER+C_GET_ETL_DETAIL+C_COLOR_ENDC+"\n"+
                      C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tВыдает детализацию по ETL-процессу\n"+
                      C_COLOR_BOLD+"Аргументы (хотя бы один должен быть заполнен):"+C_COLOR_ENDC+"\n\t"+
                      C_COLOR_OKCYAN+C_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ETL_DETAIL).get(C_ID_CONSOLE_ARG).get(C_DESC)+"\n\t"+
                      C_COLOR_OKCYAN+C_ETL_ID_CONSOLE_ARG+C_COLOR_ENDC+": "+C_CONSOLE_ARGS.get(C_GET_ETL_DETAIL).get(C_ETL_ID_CONSOLE_ARG).get(C_DESC),
    C_EXIT:"\n"+C_COLOR_HEADER+C_EXIT+C_COLOR_ENDC+"\n"+
           C_COLOR_BOLD+"Описание:"+C_COLOR_ENDC+"\n\tЗавершает работу"
}

# шаблон json для создания сущности
C_ENTITY_PARAM_TEMPLATE="""{
    "entity":"description: name of the entity, type: str",
    "description":"description: description of the entity, type: str",
    "attribute":
    [
        {
            "name":"description: name of the attribute, type: str",
            "description":"description: description of the attribute, type: str",
            "pk":"description: the attribute is the entity's primary key (0 or 1 or null), type: int",
            "datatype":"description: datatype of the attribute, type: str",
            "length":"description: length of the attribute (number or null), type: int",
            "scale":"description: scale of the attribute (number or null), type: int",
            "link_entity":"description: id of the linked entity, type: str",
            "source":
            [
                {
                    "source":"description: id of the source, type: str",
                    "schema":"description: the source schema, type: str",
                    "table":"description: the source table, type: str",
                    "column":"the source column"
                }
            ]
        }
    ]

}"""

C_ENTITY_PARAM_TEMPLATE_FILE_PATH='../Entity param.json' # путь до файла с параметрами сущности для создания
