"""
Модуль для работы через консоль
"""
from API import *
import json
from prettytable import PrettyTable
import shlex




def __get_command(p_input: str):
    """
    Определяет команду, поданную через консоль, и ее аргументы

    :param l_input: строка, поданная через консоль
    """
    # разбиваем строку по пробелам (если в кавычках слово - не разбиваем) и получаем лист слов
    l_word=[]
    for i_word in shlex.split(p_input):
        l_word.append(i_word)
    l_command = l_word[0] # первое слово - команда
    l_help_command=None
    if l_command==C_HELP and l_word.__len__()>1:
        l_help_command=l_word[1] # если команда help - второе слово будет наименование команды, по которой нужно выдать справку
        __command_checker(l_command)
    l_arg={} # словарь аргументов
    i=0
    for i_arg in l_word:
        if i_arg[0]=="-": # если слово начинается на "-" значит аргумент команды
            __arg_checker(p_command=l_command, p_arg=i_arg)
            l_arg.update(
                {
                    i_arg:l_word[i+1]
                }
            )
        i+=1

    return l_command, l_arg, l_help_command


def __command_checker(p_command: str):
    """
    Проверяет комманду на корректность

    :param p_command: команда
    """
    if p_command not in C_CONSOLE_COMMAND_LIST:
        print(C_COLOR_FAIL+"Команды "+p_command+" не существует"+C_COLOR_ENDC)
        console_input()



def __arg_checker(p_command: str, p_arg: str):
    """
    Проверяет аргумент команды на корректность

    :param p_command: команда
    :param p_arg: аргумент команды
    """
    if p_arg not in C_CONSOLE_ARGS.get(p_command):
        print(C_COLOR_FAIL+"У команды "+p_command+" не существует аргумента "+p_arg+C_COLOR_ENDC)
        console_input()


def __command_exec(p_command: str, p_arg: dict =None, p_help_command: str =None):
    """
    Выполняет команду

    :param p_command: команда
    :param p_arg: аргументы команды
    :param p_help_command: команда, по которой требуется выдать справку
    """
    l_json=None
    if p_command==C_GET_SOURCE:
        l_name=p_arg.get(C_NAME_CONSOLE_ARG)
        l_id=p_arg.get(C_ID_CONSOLE_ARG)
        l_json=get_source(p_source_name=l_name, p_source_id=l_id)
    elif p_command==C_EXIT:
        sys.exit()
    elif p_command==C_HELP:
        __help(p_command=p_help_command)
    else:
        return None
    return __print_result(p_json=l_json)

def __help(p_command: str =None):
    """
    Выдает справку по команде/по всем командам

    :param p_command: команда, по которой нужно получить справку
    """
    if p_command:
        __command_checker(p_command=p_command)
        l_desc=C_CONSOLE_COMMAND_DESC.get(p_command)
        print(l_desc)
        console_input()
    else:
        l_desc=""
        l_command=list(C_CONSOLE_COMMAND_DESC.keys())
        for i_command in l_command:
            l_desc+=C_CONSOLE_COMMAND_DESC.get(i_command)+"\n"
        l_desc=l_desc[:-1]
        print(l_desc)
        console_input()


def __print_result(p_json: json):
    """
    Переделывает результат команды из json в вид для консоли и выводит его

    :param p_json: json
    """
    l_json=json.loads(p_json)
    if l_json.get(C_ERROR):
        print(C_COLOR_FAIL+l_json.get(C_ERROR)+C_COLOR_ENDC)
        console_input()
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

    __command_exec(p_command=l_command[0], p_arg=l_command[1], p_help_command=l_command[2])

    console_input()
