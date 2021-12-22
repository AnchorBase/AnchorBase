"""
Модуль для работы через консоль
"""
from API import *
import re
import json
from prettytable import PrettyTable




def __get_command(p_input: str):
    """
    Определяет команду, поданную через консоль

    :param l_input: строка, поданная через консоль
    """
    l_first_word = re.compile(r'\w+')
    l_command = l_first_word.findall(p_input)[0]
    __command_checker(l_command)
    return l_command


def __command_checker(p_command: str):
    """
    Проверяет комманду на корректность

    :param p_command: команда
    """
    if p_command not in C_CONSOLE_COMMAND_LIST:
        print(C_COLOR_FAIL+"Команды "+p_command+" не существует"+C_COLOR_ENDC)

def __command_exec(p_command: str):
    """
    Выполняет команду

    :param p_command: команда
    """
    l_json=None
    if p_command=="get_source":
        l_json=get_source()
    elif p_command=="exit":
        sys.exit()
    else:
        return None
    return __print_result(p_json=l_json)

def __print_result(p_json: json):
    """
    Переделывает результат команды из json в вид для консоли и выводит его

    :param p_json: json
    """
    l_json=json.loads(p_json)
    l_data=l_json.get(C_DATA)
    # создаем таблицу
    # колонки таблицы
    l_col=list(l_data[0].keys()) # колонки берем из первого элемента листа
    l_table=PrettyTable(l_col) # создаем таблицу
    for i_object in l_data:
        l_row=[] # строка
        for i_col in l_col:
            l_row.append(i_object.get(i_col))
        # добавляем строку в таблицу
        l_table.add_row(l_row)

    print(l_table)

def console_input():
    """
    Выполняет команду переданную через консоль
    """
    l_input=input(
        "anchorbase: "
    )
    l_command=__get_command(l_input)

    __command_exec(p_command=l_command)

    console_input()
