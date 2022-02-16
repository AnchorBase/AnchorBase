import mysql.connector as mysql
from Constants import *

def sql_exec(
        p_server: str,
        p_database: str,
        p_user: str,
        p_password: str,
        p_port: int,
        p_sql: str,
        p_result: int =1
):
    """
    Execute queries in MySQL

    :param p_server: server
    :param p_database: db
    :param p_user: login
    :param p_password: password
    :param p_port: port
    :param p_result: flag that the output is necessary (default - 1)
    """

    l_error=None
    query_output=None
    try:
        cnct=mysql.connect(
            host=p_server,
            database=p_database,
            user=p_user,
            password=p_password,
            port=p_port
        )
    except mysql.Error as e:
        l_error=e
        return query_output, l_error
    crsr=cnct.cursor()
    try:
        crsr.execute(p_sql)
        if p_result==1:
            query_output=crsr.fetchall()
        else:
            query_output=1
    except mysql.Error as e:
        l_error=e
    finally:
        crsr.close()
        cnct.close()

    return query_output, l_error
