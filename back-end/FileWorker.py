import sys

class File:
    """
    Класс по работе с файлами
    """
    # конструктор
    def __init__(self, p_file_path : str, p_file_body =None):
        """
        :param p_file_path: путь до файла
        :param p_file_body: наполнение файла
        """
        self._file_path=p_file_path
        self._file_body=p_file_body

    @property
    def file_path(self) -> str:
        """
        Путь до файла
        """
        return self._file_path

    @property
    def file_body(self):
        """
        Наполнение файла
        """
        # если не было передано наполнение в классе - берем из существующего файла
        if self._file_body is None:
            return self.read_file()
        else:
            return self._file_body

    @file_body.setter
    def file_body(self, p_file_body_modify):
        """
        Создает/Изменяет наполнение файла
        """
        self._file_body=p_file_body_modify

    def read_file(self) :
        """
        Читает файл и кладет в переменную
        """
        l_file = open(self.file_path ,'r')
        l_file_body = l_file.read()
        l_file.close()
        return l_file_body

    def write_file(self):
        """
        Создает файл/Переписывает файл

        """
        l_file = open(self.file_path,'w')
        l_file.write(self.file_body)
        l_file.close()

    def replace_in_body(self, p_params_dict: dict):
        """
        Заменяет переменные на значения

        :param p_params_dict: словарь переменных и их значений
        :return: скорректированное наполнение файла
        """
        l_file_body_modify=self.file_body # скорректированное наполнение файла
        # получаем все ключи в словаре
        l_params_dict_keys = list(p_params_dict.keys())
        # проходимся циклом по словарю
        for i_params_dict_keys in l_params_dict_keys:
            l_params_key_value = p_params_dict.get(i_params_dict_keys,None)
            # проверяем, что значение ключа не множественное
            if type(l_params_key_value) is list:
                sys.exit("У переменной несколько значений") #TODO: реализовать вывод ошибок, как сделал Рустем
            # заменяем все места с указанием переменной значением
            l_file_body_modify = l_file_body_modify.replace(i_params_dict_keys,l_params_key_value)
        return l_file_body_modify









