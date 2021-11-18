"""
Системные объекты (Константы, ошибки и т.д.)
"""

import sys
import Constants
import contextvars

class Constant:
    """
    Константа, используемая в программе
    """

    def __init__(self, p_constant_name: str):
        """
        Конструктор

        :param p_constant_name: наименование константы
        """
        self._constant_name=p_constant_name

    @property
    def constant_name(self):
        """
        Наименование константы
        """
        return self._constant_name.upper()


    def get_all_constants(self):
        """
        Выдает список всех констант
        """
        l_object_list = dir(Constants) # получаем список всех объектов класса
        l_constant_list = [] # список только с константами
        for i_object in l_object_list:
            if i_object.upper()==i_object: # только константы имеют наименование большими буквами
                l_constant_list.append(i_object)
        return l_constant_list

    def constant_exist_checker(self, p_constant):
        """
        Проверяет, существование заданной константы
        """
        l_constants = self.get_all_constants()
        if p_constant not in l_constants:
            sys.exit("Не найдена константа "+str(p_constant)) #TODO: реализовать вывод ошибок, как сделал Рустем

    @property
    def constant_value(self):
        """
        Выдает значение запрашиваемой константы
        :param p_constant: Константа
        """
        # проверяем на наличие константы
        self.constant_exist_checker(self.constant_name)
        l_constant=Constants.get_constant_list()
        return l_constant[self.constant_name]



