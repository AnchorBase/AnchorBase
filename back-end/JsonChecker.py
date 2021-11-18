from _typeshed import NoneType
from abc import ABC, abstractmethod
from typing import Union

from backend.Metadata import Metadata
from backend.Driver import get_datatype_postgresql, is_connection_available, Driver

def get_duplicates(p_list: list) -> list:
    """
        Вспомогательный метод, возвращающий дублирующиеся значения в списке
        input:
            p_list - list of elements
    """
    l_set_no_duplicates = set()
    l_set_duplicates = set()
    for i_element in p_list:
        if i_element in l_set_no_duplicates:
            l_set_duplicates.add(i_element)
        else:
            l_set_no_duplicates.add(i_element)
    return list(l_set_duplicates)

class ABCJsonChecker(ABC):
    """
        Абстрактный класс, от которого наследуются все классы-чекеры
    """

    # Конструктор
    def __init__(self, p_json: dict):
        """
            Конструктор
            Input:
                p_json - словарь с данными для проверки
        """
        self._json = p_json

        # Список с ошибками при проверке
        self._errors = []

        # Флаг наличия критической ошибки, при True - прекращаем действие init-а
        self._critical_error_flg = False

        """
            Информация о структуре (ключи и значения json-а)
            Необходимо задавать для каждого наследуемого класса 
            для инициализации проверки формата словаря
            Имеет следующий вид: 
            (
                (
                    <название ключа - строка>,
                    <тип данных ключа - кортеж>,
                    <обязательность значения - булеан>
                ),
                ...
            )
        """
        self._json_keys_info = None

        # Шаблоны для сообщения об ошибках
        self.C_JSON_IS_NOT_OBJECT_TEMPLATE = "Проверяемый json должен быть словарем."
        self.C_JSON_KEY_NOT_FOUND_TEMPLATE = "У проверяемого json-а отсутствует ключ {0}."
        self.C_JSON_KEY_VALUE_EMPTY_TEMPLATE = "У проверяемого json-а у ключа {0} отсутствует значение."
        self.C_JSON_VALUE_INCORRECT_DATATYPE_TEMPLATE = "У проверяемого json-а у ключа {0} не соответствует заявленному типу данных."

        """
            Список ключей, которые были не найдены во время проверок
            По этим ключам нет смысла выполнять 
            дальнейшие проверки на наличие значения и на его тип
        """
        self._keys_not_found = []

    # Абстрактные методы
    @abstractmethod
    def run_check(self):
        """
            Здесь вызываются все проверки,
            относящиеся к конкретным значениям json-а на уровне строки,
            либо общие проверки
        """

    # Основные методы
    def check_json_format(self):
        """
            Проверка словаря на входе -
            проверяется наличие ключа,
            совпадение значения ключа определенному типу,
            наличие значения у ключа

            Если есть хотя бы одна ошибка, то устанавливаем флаг критической ошибки

            Все ошибки пишутся в список ошибок
        """
        for i_json_key_metadata in self._json_keys_info:
            l_key_str = i_json_key_metadata[0]
            l_datatype_tuple = i_json_key_metadata[1]
            l_value_required = i_json_key_metadata[2]

            self.helper_is_dict(p_method_name="check_json_format")
            if not self.critical_error_flg:
                self.helper_check_key_exists(
                    p_key_str=l_key_str,
                    p_method_name="check_json_format",
                )
                self.helper_check_key_datatype(
                    p_key_str=l_key_str,
                    p_datatype=l_datatype_tuple,
                    p_method_name="check_json_format"
                )
                if l_value_required:
                    self.helper_check_value_exists(
                        p_key_str=l_key_str,
                        p_method_name="check_json_format"
                    )

            if len(self.errors) > 0:
                self.critical_error_flg = True


    def add_error(self, p_error: Union[str, list]):
        """
            Добавляет ошибку или ошибки
            input:
                p_error - ошибка, может быть строкой или списком
        """
        if isinstance(p_error, (str,)):
            self._errors.append(p_error)
        elif isinstance(p_error, (list,)):
            self.add_error(p_error=p_error)

    def add_single_error(
            self,
            p_method_name: str,
            p_error_message: str,
            p_is_error_critical: bool=False
    ):
        """
            Добавляет ошибку с указанием метода и сообщением об ошибке
            input:
                p_method_name - название метода
                p_error_message - сообщение об ошибке
        """
        l_error = self.helper_get_error_message(
            p_method_name=p_method_name,
            p_error_message=p_error_message
        )
        self.add_error(p_error=l_error)
        if p_is_error_critical:
            self.critical_error_flg = True

    # Хэлперы
    def helper_get_class_name(self) -> str:
        """
            Возвращает название класса
        """
        return self.__class__.__name__

    def helper_get_error_message(
            self,
            p_method_name: str,
            p_error_message: str
    ) -> str:
        """
            Возвращает отформатированное сообщение об ошибке
            input:
                p_method_name - название метода
                p_error_message - сообщение об ошибке
        """
        return "::".join([self.helper_get_class_name, p_method_name, p_error_message])

    def helper_is_dict(
            self,
            p_method_name: str="check_json_format"
    ):
        """
            Проверяет, является ли json - словарем
            input:
                p_method_name - название метода
        """
        if not isinstance(self.json, (dict,)):
            self.add_single_error(
                p_method_name=p_method_name,
                p_error_message=self.C_JSON_IS_NOT_OBJECT_TEMPLATE,
                p_is_error_critical=True
            )

    def helper_check_key_exists(
            self,
            p_key_str: str,
            p_method_name: str="check_json_format",
    ):
        """
            Проверяет наличие ключа в словаре (json)
            input:
                p_key_str - название ключа
                p_method_name - название метода
        """
        try:
            self.json[p_key_str]
        except KeyError:
            self.add_single_error(
                p_method_name=p_method_name,
                p_error_message=self.C_JSON_KEY_NOT_FOUND_TEMPLATE.format(p_key_str)
            )
            """
                Заносим ненайденные ключи в список, 
                чтобы не делать по ним проверки типов и заполненности значения 
            """
            self._keys_not_found.append(p_key_str)

    def helper_check_value_exists(
            self,
            p_key_str: str,
            p_method_name: str="check_json_format"
    ):
        """
            Проверяет наличие заполненного значения
            input:
                p_key_str - название ключа
                p_method_name - название метода
        """
        # Если ключ есть в ненайденных ключах, то проверка не имеет смысл
        # Проверка на наличие значения + проверка на то, что найденное значение не является boolean и принимает значение False
        if p_key_str not in self._keys_not_found \
                and not self.json[p_key_str] \
                and not isinstance(self.json[p_key_str], (bool,)):
            self.add_single_error(
                p_method_name=p_method_name,
                p_error_message=self.C_JSON_KEY_VALUE_EMPTY_TEMPLATE.format(p_key_str)
            )

    def helper_check_key_datatype(
            self,
            p_key_str: str,
            p_datatype: tuple,
            p_method_name: str="check_json_format"
    ):
        """
            Проверяет соответствие значения ключа - указанному типу данных
            input:
                p_key_str - название ключа
                p_datatype - тип данных
                p_method_name - название метода
        """
        # Если ключ не найден, то проверка не имееет смысл
        if p_key_str not in self._keys_not_found:
            try:
                l_value = self.json[p_key_str]
                if not isinstance(l_value, NoneType) and not isinstance(l_value, p_datatype):
                    self.add_single_error(
                        p_method_name=p_method_name,
                        p_error_message=self.C_JSON_VALUE_INCORRECT_DATATYPE_TEMPLATE.format(p_key_str)
                    )
            except KeyError:
                pass

    def select_meta_one_row(
            self,
            p_table_name: str,
            p_no_data_found_message: str,
            p_too_many_rows_message: str,
            p_method_name: str,
            p_uuid: str=None,
            p_where_predicate: dict=None
    ) -> Union[list]:
        """
            Усовершенствованный вариант select_meta,
            который пишет в словарь с ошибками,
            если найдено ни одной строки или более одной строки
            input:
                p_table_name - название таблицы в метаданных
                p_no_data_found_message - сообщение об ошибке, если ни найдено ни одной строки
                p_too_many_rows_message - сообщение об ошибке, если найдено более одной строки
                p_method_name - название метода
                p_uuid - идентификатор строки (опциональный параметр)
                p_where_predicate - словарь для генерации where предиката
            output:
                Список из словарей, представляющих из себя строки таблицы
                Либо NoneType, если была ошибка
        """
        l_metadata_result = Metadata.select_meta(
            object=p_table_name,
            id=p_uuid,
            object_attr=p_where_predicate
        )
        if len(l_metadata_result) == 0:
            self.add_single_error(
                p_method_name=p_method_name,
                p_error_message=p_no_data_found_message,
                p_is_error_critical=True
            )
        elif len(l_metadata_result) > 1:
            self.add_single_error(
                p_method_name=p_method_name,
                p_error_message=p_too_many_rows_message,
                p_is_error_critical=True
            )
        else:
            return l_metadata_result

    # Геттеры и сеттеры
    @property
    def json(self) -> dict:
        """
            Словарь с данными
        """
        return self._json

    @property
    def errors(self) -> list:
        """
            Список с ошибками проверок
        """
        return self._errors

    @property
    def critical_error_flg(self):
        """
            Флаг наличия критической ошибки
        """
        return self._critical_error_flg

    @critical_error_flg.setter
    def critical_error_flg(self, value: bool):
        """
            Флаг наличия критической ошибки
        """
        self._critical_error_flg = value


class SourceChecker(ABCJsonChecker):
    """
        Проверка источника
    """

    def __init__(self, p_json: dict, p_entity_name: str, p_attribute_name: str):
        """
            input:
                p_json - json с данными
                p_entity_name - название сущности
                p_attrbute_name - название атрибута
        """
        super().__init__(p_json=p_json)
        self._entity_name = p_entity_name
        self._attribute_name = p_attribute_name

        self._connection_type = None
        self._connection_object = None

        l_error_prefix = "Сущность={0};Атрибут={1};".format(self.entity_name, self.attribute_name)

        self.C_JSON_IS_NOT_OBJECT_TEMPLATE = l_error_prefix + "Json c информацией об источнике должен быть словарем."
        self.C_JSON_KEY_NOT_FOUND_TEMPLATE = l_error_prefix + "У проверяемого json-а c информацией об источнике отсутствует ключ {0}."
        self.C_JSON_KEY_VALUE_EMPTY_TEMPLATE = l_error_prefix + "У проверяемого json-а с информацией об источнике у ключа {0} отсутствует значение."
        self.C_JSON_VALUE_INCORRECT_DATATYPE_TEMPLATE = l_error_prefix + "У проверяемого json-а с информацией об источнике значение ключа {0} не соответствует заявленному типу данных."

        self.C_SOURCE_NOT_FOUND_METADATA = l_error_prefix + "Источник {0} не найден в метаданных."
        self.C_SOURCE_TOO_MANY_ROWS_METADATA = l_error_prefix + "Найдено более одного источника с названием {0}."
        self.C_SOURCE_UNAVAILABLE = l_error_prefix + "Источник {0} недоступен для подключения."
        self.C_SOURCE_TABLE_NOT_FOUND = l_error_prefix + "На источнике {0} не найдена таблица {1}.{2}."
        self.C_ATTRIBUTE_NOT_FOUND = l_error_prefix + "На источнике {0} у таблицы {1}.{2} не найдено атрибута {3}."

        self._json_keys_info = (
            ("name", (str,), True),
            ("schema", (str,), True),
            ("table", (str,), True),
            ("column", (str,), True),
        )

        self.check_json_format()
        if self.critical_error_flg:
            return

        self.run_check()

    def run_check(self):
        """
            Проверки, не относящиеся к проверкам входного json-а
        """
        self.check_source_exists()
        if not self.critical_error_flg:
            self.check_table_column_exists()

    def check_source_exists(self):
        """
            Проверяет наличие источника в метаданных и его доступность
            Кэширует часть информации из метаданных, чтобы не делать еще запросы к БД
        """
        l_meta_source_result = self.select_meta_one_row(
            p_table_name="source",
            p_no_data_found_message=self.C_SOURCE_NOT_FOUND_METADATA.format(
                self.name
            ),
            p_too_many_rows_message=self.C_SOURCE_TOO_MANY_ROWS_METADATA.format(
                self.name
            ),
            p_method_name="check_source_exists",
            p_where_predicate={"name": self.name}
        )
        if self.critical_error_flg:
            return

        l_connection_check_result = is_connection_available(
            p_source_uuid=l_meta_source_result[0]["id"]
        )

        if not l_connection_check_result:
            self.add_single_error(
                p_method_name="check_source_exists",
                p_error_message=self.C_SOURCE_UNAVAILABLE.format(self.name),
                p_is_error_critical=True
            )
        else:
            self._connection_type = l_meta_source_result[0]["type"]
            self._connection_object = {
                "database": l_meta_source_result[0]["database"],
                "schema": self.schema,
                "table": self.table
            }

    def check_table_column_exists(self):
        """
            Проверка существования таблицы на источнике
            Проверка существования колонки у таблицы на источнике - решил совместить
        """
        l_source_table_data_dictionary_information = Driver.select_db_object(
            cnct_attr=self.connection_type,
            object=self.connection_object
        )

        if not l_source_table_data_dictionary_information:
            self.add_single_error(
                p_method_name="check_table_column_exists",
                p_error_message=self.C_SOURCE_TABLE_NOT_FOUND.format(
                    self.name, self.schema, self.table
                )
            )
            self._critical_error_flg = True
        else:
            # 5 по счету элемент из запроса, генерирующегося к словарю данных - это название колонки
            l_columns = {i_attribute[4] for i_attribute in l_source_table_data_dictionary_information}
            if self.column not in l_columns:
                self.add_single_error(
                    p_method_name="check_table_column_exists",
                    p_error_message=self.C_ATTRIBUTE_NOT_FOUND.format(
                        self.name, self.schema, self.table, self.column
                    )
                )
                self._critical_error_flg = True

    @property
    def entity_name(self) -> str:
        """
            Название сущности
        """
        return self._entity_name

    @property
    def attribute_name(self) -> str:
        """
            Название атрибута
        """
        return self._attribute_name

    @property
    def name(self) -> str:
        """
            Название источника
        """
        return self.json["name"]

    @property
    def schema(self):
        """
            Схема таблицы на источнике
        """
        return self._json["schema"]

    @property
    def table(self):
        """
            Название таблицы на источнике
        """
        return self._json["table"]

    @property
    def column(self):
        """
            Название колонки у таблицы
        """
        return self._json["column"]

    @property
    def connection_type(self):
        """
            Тип подключения
        """
        return self._connection_type

    @property
    def connection_object(self):
        """
            Объект для подключения
        """
        return self._connection_object


class AttributeChecker(ABCJsonChecker):
    """
        Проверка атрибута
    """

    def __init__(self, p_json: dict, p_entity_name: str):
        """
            input:
                p_json - json с данными
                p_entity_name - название сущности
        """
        super().__init__(p_json=p_json)
        self._entity_name = p_entity_name

        l_error_prefix = "Сущность={0};".format(self.entity_name)

        self.C_JSON_IS_NOT_OBJECT_TEMPLATE = l_error_prefix + "Json c информацией об атрибуте должен быть словарем."
        self.C_JSON_KEY_NOT_FOUND_TEMPLATE = l_error_prefix + "У проверяемого json-а c информацией об атрибуте отсутствует ключ {0}."
        self.C_JSON_KEY_VALUE_EMPTY_TEMPLATE = l_error_prefix + "У проверяемого json-а с информацией об атрибуте у ключа {0} отсутствует значение."
        self.C_JSON_VALUE_INCORRECT_DATATYPE_TEMPLATE = l_error_prefix + "У проверяемого json-а с информацией об атрибуте значение ключа {0} не соответствует заявленному типу данных."

        self.C_DATATYPE_WITHOUT_SIZE = (
            "boolean", "bytea", "integer", "money", "smallint", "text"
        )

        self.C_CHAR_MIN_LENGTH = 1
        self.C_CHAR_MAX_LENGTH = 65535
        self.C_DECIMAL_MIN_PRECISION = 1
        self.C_DECIMAL_MAX_PRECISION = 1000
        self.C_TIMESTAMP_MIN_SCALE = 0
        self.C_TIMESTAMP_MAX_SCALE = 6

        self.C_DATATYPE_UNKNOWN_TEMPLATE = l_error_prefix + "Обнаружен неизвестный тип данных {0}."
        self.C_DATATYPE_SIZE_NOT_ALLOWED_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} указаны length и scale"
        self.C_DATATYPE_LENGTH_NOT_ALLOWED_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} не должна быть указана длина."
        self.C_DATATYPE_SCALE_NOT_ALLOWED_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} не должна быть указана точность после запятой."
        self.C_DATATYPE_SCALE_NOT_SPECIFIED_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} не указана точность после запятой."
        self.C_CHAR_LENGTH_NOT_SPECIFIED_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} не указана длина."
        self.C_CHAR_INCORRECT_LENGTH_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} длина должна быть от {2} до {3}."
        self.C_DECIMAL_PRECISION_NOT_SPECIFIED_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} не указана точность."
        self.C_DECIMAL_INCORRECT_PRECISION_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} точность должна быть от {3} до {4}."
        self.C_DECIMAL_INCORRECT_SCALE_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} количество знаков после запятой должно быть от {2} до {3}."
        self.C_TIMESTAMP_INCORRECT_SCALE_TEMPLATE = l_error_prefix + "У типа данных {0} атрибута {1} точность должна быть от {2} до {3}."

        self._json_keys_info = (
            ("name", (str,), True),
            ("desc", (str,), False),
            ("pk", (bool,), False),
            ("foreign_entity", (str,), False),
            ("datatype", (str,), True),
            ("length", (int,), False),
            ("scale", (int,), False),
            ("source", (list,), True)
        )

        self.check_json_format()
        if self.critical_error_flg:
            return

            # Проверка источников
        self._source_checker = []
        for i_source in self.source:
            l_source = SourceChecker(
                p_json=i_source,
                p_entity_name=self.entity_name,
                p_attribute_name=self.name
            )
            self.errors += l_source.errors
            if not l_source.critical_error_flg:
                self._source_checker.append(l_source)

        self.run_check()

    def run_check(self):
        """
            Сквозные проверки атрибута
        """
        self.check_datatype()

    def check_datatype(self):
        """
            Проверка размерности у типа данных

            Пока исходим из того, что здесь проверяем только типы данных PostgreSQL

            Небольшой познавательный ликбез на тему типов данных в PostgreSQL:
                boolean - не нужен length, scale. 1 байт - true | false | unknown
                bytea - не нужен length, scale. Бинарная строка до 1 гб
                char - нужен length, не нужен scale - максимальный length - 65535
                decimal - эквивалентен numeric, length и scale - опциональные, нельзя указать scale отдельно,
                          length от 1 до 1000, scale от 0 до length
                integer - не нужен length, scale
                money - не нужен length, scale
                smallint - не нужен length, scale
                text - не нужен length, scale
                timestamp - length от 0 до 6, scale - не нужен
                varbinary - в документации написано, что надо использовать bytea
                varchar - если length указан, то от 1 до 65535, если не указан, то это text
        """

        # Проверка того, что указанный тип данных допустим
        if self.datatype not in get_datatype_postgresql():
            self.add_single_error(
                p_method_name="check_datatype",
                p_error_message=self.C_DATATYPE_UNKNOWN_TEMPLATE.format(self.datatype)
            )
            # Проверка на то, что у типа без размерности не указана размерность
        elif self.datatype in self.C_DATATYPE_WITHOUT_SIZE:
            if self.length is not None or self.scale is not None:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_message=self.C_DATATYPE_SIZE_NOT_ALLOWED_TEMPLATE.
                        format(self.datatype, self.name)
                )
        # Проверки типа данных char, varchar
        elif self.datatype in ("char", "varchar"):
            # Проверка того, что длина указана
            if self.length is None:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_message=self.C_CHAR_INCORRECT_LENGTH_TEMPLATE
                        .format(self.datatype, self.name)
                )
            # Проверка того, что строка имеет допустимую длину
            elif not self.C_CHAR_MIN_LENGTH <= self.length <= self.C_CHAR_MAX_LENGTH:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_message=self.C_CHAR_INCORRECT_LENGTH_TEMPLATE
                        .format(self.datatype, self.name, self.C_CHAR_MIN_LENGTH, self.C_CHAR_MAX_LENGTH)
                )
            # Проверка того, что не указан scale
            if self.scale is not None:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_message=self.C_DATATYPE_SCALE_NOT_SPECIFIED_TEMPLATE
                        .format(self.datatype, self.name)
                )
        # Проверки типа данных decimal
        elif self.datatype == "decimal":
            # Проверка отсутствия precision
            if self.length is None:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_message=self.C_DECIMAL_PRECISION_NOT_SPECIFIED_TEMPLATE
                        .format(self.datatype, self.name)
                )
            # Проверка допустимости значения precision
            elif not self.C_DECIMAL_MIN_PRECISION <= self.length <= self.C_DECIMAL_MAX_PRECISION:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_message=self.C_DECIMAL_INCORRECT_PRECISION_TEMPLATE
                        .format(self.datatype, self.name, self.C_DECIMAL_MIN_PRECISION, self.C_DECIMAL_MIN_PRECISION)
                )
            # Проверка допустимости значения scale
            elif self.length is not None and self.scale is not None \
                    and 0 <= self.scale <= self.length:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_message=self.C_DECIMAL_INCORRECT_SCALE_TEMPLATE
                        .format(self.datatype, self.name, "0", self.length)
                )
        elif self.datatype == "timestamp":
            # Проверка наличия количества знаков после запятой
            if self.scale is None:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_description=self.C_DATATYPE_SCALE_NOT_SPECIFIED_TEMPLATE
                        .format(self.datatype, self.name)
                )
            # Проверка допустимости значения scale
            elif not self.C_TIMESTAMP_MIN_SCALE <= self.scale <= self.C_TIMESTAMP_MAX_SCALE:
                self.add_single_error(
                    p_method_name="check_datatype",
                    p_error_description=self.C_TIMESTAMP_INCORRECT_SCALE_TEMPLATE
                        .format(self.datatype, self.name, self.C_TIMESTAMP_MIN_SCALE, self.C_TIMESTAMP_MAX_SCALE)
                )

    @property
    def entity_name(self) -> str:
        """
            Название сущности
        """
        return self._entity_name

    @property
    def name(self) -> str:
        """
            Название атрибута
        """
        return self.json["name"]

    @property
    def pk(self) -> bool:
        """
            Является ли атрибут первичным ключем
        """
        return self.json["pk"]

    @property
    def foreign_entity(self) -> str:
        """
            Название внешней сущности
        """
        return self.json["foreign_entity"]

    @property
    def datatype(self) -> str:
        """
            Тип данных
        """
        return self.json["datatype"]

    @property
    def length(self) -> int:
        """
            Количество символов (длина) типа данных
        """
        return self.json["length"]

    @property
    def scale(self) -> int:
        """
            Количество знаков после запятой у типа данных
        """
        return self.json["scale"]

    @property
    def source(self) -> list:
        """
            Список источников атрибута
        """
        return self.json["source"]

    @property
    def source_checker(self) -> list:
        """
            Список экземпляров класса SourceChecker
        """
        return self._source_checker


class EntityChecker(ABCJsonChecker):
    """
        Проверка сущности
    """

    def __init__(self, p_json: dict):
        """
            input:
                p_json - json с данными
        """
        super().__init__(p_json=p_json)

        self.C_JSON_IS_NOT_OBJECT_TEMPLATE = "JSON с информацией о сущности должен быть словарем."
        self.C_JSON_KEY_NOT_FOUND_TEMPLATE = "У проверяемого json-а c информацией о сущности отсутствует ключ {0}."
        self.C_JSON_KEY_VALUE_EMPTY_TEMPLATE = "У проверяемого json-а с информацией о сущности у ключа {0} отсутствует значение."
        self.C_JSON_VALUE_INCORRECT_DATATYPE_TEMPLATE = "У проверяемого json-а с информацией о сущности значение ключа {0} не соответствует заявленному типу данных."

        self.C_PK_NOT_FOUND = "У сущности {0} не найдено ни одного атрибута с признаком ключа."
        self.C_ATTRIBUTE_DUPVAL_FOUND = "У сущности {0} следующие атрибуты повторяются более одного раза: {1}."
        self.C_ENTITY_ALREADY_EXISTS = "Сущность {0} уже существует и зарегистрирована в метаданных."

        self._json_keys_info = (
            ("name", (str,), True),
            ("desc", (str,), False),
            ("attribute", (list,), True)
        )

        self.check_json_format()
        if self.critical_error_flg:
            return

        self._attribute_checker = []
        for i_attribute in self.attrbute:
            l_attribute = AttributeChecker(
                p_json=i_attribute,
                p_entity_name=self.name
            )
            self.errors += l_attribute.errors
            if not l_attribute.critical_error_flg:
                self._attribute_checker.append(l_attribute)

        self.run_check()

    def run_check(self):
        """
            Проверки на уровне сущности
        """
        self.check_pk_exists()
        self.check_attribute_duplicate_value()
        self.check_entity_exists()

    def check_pk_exists(self):
        """
            Проверка наличия ключа среди атрибутов сущности
        """
        for i_attribute in self.attribute_checker:
            if i_attribute.pk:
                return
        self.add_single_error(
            p_method_name="check_pk_exists",
            p_error_message=self.C_PK_NOT_FOUND.format(self.name)
        )

    def check_attribute_duplicate_value(self):
        """
            Проверка наличия дублей у атрибутов сущности
        """
        l_attribute_names = [i_attribute.name for i_attribute in self.attribute_checker]
        l_duplicate_values = get_duplicates(p_list=l_attribute_names)
        if len(l_duplicate_values):
            self.add_single_error(
                p_method_name="check_attribute_duplicate_values",
                p_error_message=self.C_ATTRIBUTE_DUPVAL_FOUND.format(
                    self.name, " ,".join(l_duplicate_values)
                )
            )

    def check_entity_exists(self):
        """
            Проверка наличия сущности в метаданных
        """
        l_meta_entity_result = Metadata.select_meta(
            object="entity",
            object_attr={"name": self.name}
        )
        if l_meta_entity_result:
            self.add_single_error(
                p_method_name="check_entity_exists",
                p_error_message=self.C_ENTITY_ALREADY_EXISTS.format(self.name)
            )

    @property
    def name(self) -> str:
        """
            Название сущности
        """
        return self.json["name"]

    @property
    def attrbute(self) -> list:
        """
            Атрибуты сущности
        """
        return self.json["atttribute"]

    @property
    def attribute_checker(self) -> list:
        """
            Экземпляры классам AttributeChecker
        """
        return self._attribute_checker

class ModelChecker(ABCJsonChecker):
    """
        Проверка списка из сущностей
    """

    def __init__(self, p_json: list):
        """
            Конструктор
            input: p_json - список с данными
        """
        super().__init__(p_json=p_json)

        self.C_JSON_IS_NOT_A_LIST = "Передаваемые сущности для описания модели не являются списком."
        self.C_ENTITY_DUPLICATE_NAMES = "Следующие названия сущностей встречаются более одного раза: {0}"
        self.C_FOREIGN_ENTITY_NOT_EXIST = "Следующие сущности, указанные как внешние не создаются в модели и не зарегистрированы в метаданных: {0}"

        if not isinstance(self.json, (list,)):
            self.add_single_error(
                p_method_name="init",
                p_error_message=self.C_JSON_IS_NOT_LIST,
                p_is_error_critical=True
            )
            return

        self._entity_checker = []
        for i_entity in self.json:
            l_entity = EntityChecker(p_json=i_entity)
            self.errors += l_entity.errors
            if not l_entity.critical_error_flg:
                self._entity_checker.append(l_entity)

        # Необходимо получить список всех имен сущностей, список всех внешних сущностей
        self._entity_name_list = []
        self._foreign_entity_name_set = set()

        for i_entity in self.entity_checker:
            self._entity_name_list.append(i_entity.name)
            for j_attribute in i_entity.attribute_checker:
                self._foreign_entity_name_set.add(j_attribute.foreign_entity)

        self.run_check()

    def run_check(self):
        """
            Сквозные проверки на уровне списка сущностей (модели)
        """
        self.check_entity_duplicate_names()
        self.check_foreign_entity_exists()

    def check_entity_duplicate_names(self):
        """
            Проверка наличия повторяющихся имен сущностей
        """
        l_duplicates = get_duplicates(p_list=self._entity_name_list)
        if l_duplicates:
            self.add_single_error(
                p_method_name="check_entity_duplicate_names",
                p_error_message=self.C_ENTITY_DUPLICATE_NAMES.format(
                    ", ".join(l_duplicates)
                )
            )

    def check_foreign_entity_exists(self):
        """
            Проверка того, что все указанные внешние сущности в модели
            или создаются в модели, или зарегистрированы в метаданных
        """
        l_not_found_foreign_entities = []

        for i_foreign_entity_name in self._foreign_entity_name_set:

            if i_foreign_entity_name not in self._entity_name_list:
                l_meta_entity_result = Metadata.select_meta(
                    object="entity",
                    object_attr={"name": i_foreign_entity_name}
                )
                if not l_meta_entity_result:
                    l_not_found_foreign_entities.append(i_foreign_entity_name)

        if l_not_found_foreign_entities:
            self.add_single_error(
                p_method_name="check_foreign_entity_exists",
                p_error_message=self.C_FOREIGN_ENTITY_NOT_EXIST.format(
                    ", ".join(l_not_found_foreign_entities)
                )
            )

    @property
    def entity_checker(self):
        """
            Список экземпляров класса по проверке сущностей
        """
        return self._entity_checker
