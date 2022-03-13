"""
Модуль для работы через консоль
"""
from API import *
from FileWorker import File
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
    l_cmnd_num=0 # порядковый номер последнего слова в команде
    for i, i_word in enumerate(shlex.split(p_input)):
        if i_word.__len__()==0: # слово пустое
            l_word.append(i_word)
            continue
        if i_word[0]=="-": # если слово начинается на "-" значит аргумент команды
            if l_cmnd_num==0:
                l_cmnd_num=i
            if l_cmnd_num==0:
                print(C_COLOR_FAIL+"Неверно заданная команда"+C_COLOR_ENDC)
                console_input()
        l_word.append(i_word)
    l_command=""
    if l_cmnd_num==0: # если нет аргументов
        l_cmnd_num=l_word.__len__()
    # определяем команду
    for i_word_num in range(l_cmnd_num):
        l_command+=" "+l_word[i_word_num]
    l_command=l_command[1:]
    __command_checker(l_command)
    l_help_command=None
    # if l_command==C_HELP and l_word.__len__()>1:
    #     l_help_command=l_word[1] # если команда help - второе слово будет наименование команды, по которой нужно выдать справку
    l_arg={} # словарь аргументов
    l_arg_list=[] # список аргументов
    i=0
    for i_arg in l_word:
        if i_arg.__len__()>0 and i_arg[0]=="-": # если слово начинается на "-" значит аргумент команды
            __arg_checker(p_command=l_command, p_arg=i_arg)
            if i_arg==C_HELP:
                l_help_command=l_command
                break
            l_arg.update(
                {
                    i_arg:l_word[i+1]
                }
            )
            l_arg_list.append(i_arg)
        i+=1
    if not l_help_command: # проверяем наличие необходимых аргументов, если help не указан
        __neccessary_args_checker(p_command=l_command, p_arg=l_arg_list)

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
    if not C_CONSOLE_ARGS.get(p_command) and p_arg!=C_HELP:
        print(C_COLOR_FAIL+"У команды "+p_command+" не существует аргумента "+p_arg+C_COLOR_ENDC)
        console_input()
    if C_CONSOLE_ARGS.get(p_command) and p_arg not in list(C_CONSOLE_ARGS.get(p_command).keys()) and p_arg!=C_HELP:
        print(C_COLOR_FAIL+"У команды "+p_command+" не существует аргумента "+p_arg+C_COLOR_ENDC)
        console_input()

def __neccessary_args_checker(p_command: str, p_arg: list):
    """
    Проверяет, что у команды указаны все необходимые аргументы

    :param p_command: команда
    :param p_arg: лист указанных аргументов
    """
    if C_CONSOLE_ARGS.get(p_command):
        for i_arg in list(C_CONSOLE_ARGS.get(p_command).keys()):
                if C_CONSOLE_ARGS.get(p_command).get(i_arg).get(C_NOT_NULL)==1 \
                    and i_arg not in p_arg:  # если аргумент обязательный, но его нет в предоставленном списке
                    print(C_COLOR_FAIL+"У команды "+p_command+" не указан аргумент "+i_arg+C_COLOR_ENDC)
                    console_input()


def __command_exec(p_command: str, p_arg: dict =None, p_help_command: str =None):
    """
    Выполняет команду

    :param p_command: команда
    :param p_arg: аргументы команды
    :param p_help_command: команда, по которой требуется выдать справку
    """
    l_json=None
    if p_help_command:
        __help(p_command=p_help_command)
    elif p_command==C_GET_SOURCE:
        l_json=get_source(
            p_source_name=p_arg.get(C_NAME_CONSOLE_ARG),
            p_source_id=p_arg.get(C_ID_CONSOLE_ARG)
        )
    elif p_command==C_ADD_SOURCE:
        l_json=add_source(
            p_name=p_arg.get(C_NAME_CONSOLE_ARG),
            p_desc=p_arg.get(C_DESC_CONSOLE_ARG),
            p_server=p_arg.get(C_SERVER_CONSOLE_ARG),
            p_database=p_arg.get(C_DATABASE_CONSOLE_ARG),
            p_user=p_arg.get(C_USER_CONSOLE_ARG),
            p_password=p_arg.get(C_PASSWORD_CONSOLE_ARG),
            p_port=p_arg.get(C_PORT_CONSOLE_ARG),
            p_type=p_arg.get(C_TYPE_CONSOLE_ARG)
        )
    elif p_command==C_ALTER_SOURCE:
        l_json=update_source(
            p_id=p_arg.get(C_ID_CONSOLE_ARG),
            p_name=p_arg.get(C_NAME_CONSOLE_ARG),
            p_desc=p_arg.get(C_DESC_CONSOLE_ARG),
            p_server=p_arg.get(C_SERVER_CONSOLE_ARG),
            p_database=p_arg.get(C_DATABASE_CONSOLE_ARG),
            p_user=p_arg.get(C_USER_CONSOLE_ARG),
            p_password=p_arg.get(C_PASSWORD_CONSOLE_ARG),
            p_port=p_arg.get(C_PORT_CONSOLE_ARG),
            p_type=p_arg.get(C_TYPE_CONSOLE_ARG)
        )
    elif p_command==C_GET_SOURCE_TYPE:
        l_json=get_source_type()
    elif p_command==C_GET_ENTITY:
        l_json=get_entity(
            p_name=p_arg.get(C_NAME_CONSOLE_ARG),
            p_id=p_arg.get(C_ID_CONSOLE_ARG)
        )
    elif p_command==C_GET_ENTITY_ATTR:
        l_json=get_entity_attr(
            p_id=p_arg.get(C_ID_CONSOLE_ARG),
            p_name=p_arg.get(C_NAME_CONSOLE_ARG),
            p_entity=p_arg.get(C_ENTITY_CONSOLE_ARG)
        )
    elif p_command==C_GET_ATTR_SOURCE:
        l_json=get_attr_source(
            p_id=p_arg.get(C_ID_CONSOLE_ARG),
            p_name=p_arg.get(C_NAME_CONSOLE_ARG),
            p_entity=p_arg.get(C_ENTITY_CONSOLE_ARG),
            p_source_id=p_arg.get(C_SOURCE_ID_CONSOLE_ARG)
        )
    elif p_command==C_GET_ENTITY_SOURCE:
        l_json=get_entity_source(
            p_id=p_arg.get(C_ID_CONSOLE_ARG),
            p_name=p_arg.get(C_NAME_CONSOLE_ARG),
            p_source_id=p_arg.get(C_SOURCE_ID_CONSOLE_ARG)
        )
    elif p_command==C_START_JOB:
        l_json=start_job(
            p_entity=p_arg.get(C_ENTITY_CONSOLE_ARG),
            p_entity_attribute=p_arg.get(C_ENTITY_ATTR_CONSOLE_ARG)
        )
    elif p_command==C_GET_LAST_ETL:
        l_json=get_last_etl()
    elif p_command==C_GET_ETL_HIST:
        l_json=get_etl_hist(p_date=p_arg.get(C_DATE_CONSOLE_ARG), p_etl=p_arg.get(C_ETL_ID_CONSOLE_ARG))
    elif p_command==C_GET_ETL_DETAIL:
        l_json=get_etl_detail(
            p_etl=p_arg.get(C_ID_CONSOLE_ARG),
            p_etl_id=p_arg.get(C_ETL_ID_CONSOLE_ARG)
        )
    elif p_command==C_ADD_ENTITY:
        # считываем содержимое файла
        if p_arg.get(C_FILE_CONSOLE_ARG): # если пользователь указал файл с параметрами сущности
            l_file=File(p_file_path=p_arg.get(C_FILE_CONSOLE_ARG))
            l_entity_param=l_file.file_body
            l_json=create_entity(p_json=l_entity_param)
        else: #  если пользователь не указал конкретный файл - передаем шаблон в json
            print(C_ENTITY_PARAM_TEMPLATE+"\n"+C_COLOR_WARNING+"Вставьте параметры сущности в соответствии с шаблоном выше в файл '..\Entity param.json' и нажмите в консоли любую кнопку"+C_COLOR_ENDC)
            input()
            l_entity_param=File(p_file_path=C_ENTITY_PARAM_TEMPLATE_FILE_PATH)
            l_json=create_entity(p_json=l_entity_param.file_body)
    elif p_command==C_ALTER_ENTITY:
        # формируем json
        l_input_json={
            C_ID:p_arg.get(C_ID_CONSOLE_ARG)
        }
        if p_arg.get(C_NAME_CONSOLE_ARG):
                l_input_json.update({C_ENTITY:p_arg.get(C_NAME_CONSOLE_ARG)})
        if p_arg.get(C_DESC_CONSOLE_ARG):
            l_input_json.update({C_DESC:p_arg.get(C_DESC_CONSOLE_ARG)})
        l_input_json=json.dumps(l_input_json)
        l_json=alter_entity(p_json=l_input_json)
    elif p_command==C_DROP_ENTITY:
        # формируем json
        l_input_json={
            C_ID:p_arg.get(C_ID_CONSOLE_ARG)
        }
        l_input_json=json.dumps(l_input_json)
        l_json=drop_entity(p_json=l_input_json)
    elif p_command==C_GET_META_CONFIG:
        l_json=get_meta_config()
    elif p_command==C_UPDATE_META_CONFIG:
        l_json=update_meta_config(
            p_server=p_arg.get(C_SERVER_CONSOLE_ARG),
            p_database=p_arg.get(C_DATABASE_CONSOLE_ARG),
            p_user=p_arg.get(C_USER_CONSOLE_ARG),
            p_password=p_arg.get(C_PASSWORD_CONSOLE_ARG),
            p_port=p_arg.get(C_PORT_CONSOLE_ARG)
        )
    elif p_command==C_CREATE_META:
        l_input=input("Все существующие метаданные будут удалены. Продолжить? (y|n):")
        if l_input=='y':
            l_json=create_meta()
        else:
            return None
    elif p_command==C_GET_DWH_CONFIG:
        l_json=get_dwh_config()
    elif p_command==C_UPDATE_DWH_CONFIG:
        l_json=update_dwh_config(
            p_server=p_arg.get(C_SERVER_CONSOLE_ARG),
            p_database=p_arg.get(C_DATABASE_CONSOLE_ARG),
            p_user=p_arg.get(C_USER_CONSOLE_ARG),
            p_password=p_arg.get(C_PASSWORD_CONSOLE_ARG),
            p_port=p_arg.get(C_PORT_CONSOLE_ARG)
        )
    elif p_command==C_CREATE_DWH:
        l_input=input("Все существующие таблицы ХД будут удалены. Продолжить? (y|n):")
        if l_input=='y':
            l_json=create_dwh_ddl()
        else:
            return None
    elif p_command==C_EXIT:
        sys.exit()
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
    if l_json.get(C_MESSAGE):
        print(C_COLOR_WARNING+l_json.get(C_MESSAGE)+C_COLOR_ENDC)
    l_data=l_json.get(C_DATA)
    # создаем таблицу
    # колонки таблицы
    if l_data:
        l_col=list(l_data[0].keys()) # колонки берем из первого элемента листа
        l_table=PrettyTable(l_col) # создаем таблицу
        for i_object in l_data:
            l_row=[] # строка
            for i_col in l_col:
                l_row.append(i_object.get(i_col))
            # добавляем строку в таблицу
            l_table.add_row(l_row)
        l_table.align='l' #выравниваем по левому краю
        print(l_table)

def console_input():
    """
    Выполняет команду переданную через консоль
    """
    try:
        l_input=input(
            "anchorbase: "
        )
        l_command=__get_command(l_input)

        __command_exec(p_command=l_command[0], p_arg=l_command[1], p_help_command=l_command[2])
    except Exception as e:
        print(C_COLOR_FAIL+str(e.args[0])+C_COLOR_ENDC)


    console_input()


