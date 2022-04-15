import OData
import json
from Constants import *
from SystemObjects import *

def get_response(
        p_server: str,
        p_database: str,
        p_port: int,
        p_table: str =None,
        p_attribute_list: list =None,
        p_user: str =None,
        p_password: str =None,
        p_https: int =None
):
    """
    Get the data from 1C web-server

    :param p_server: 1C server
    :param p_database: information base 1C
    :param p_port: port
    :param p_table: data catalog
    :param p_attribute_list: list of attributes to get from 1C
    :param p_user: 1C login which is used while connecting to information base
    :param p_password: 1C password which is used while connecting to information base
    :return: tuple with data or error text
    """
    l_query_output=None
    l_error=None
    # create the connection string
    l_connection_string=p_server+":"+str(p_port)+"/"+p_database+"/"+C_1C_CONNECTION_STRING
    if not p_table: # if table/catalog is not mentioned - only call the odata function. If error occurs, error will be raised in odata function
        OData.get_response(p_connection_string=l_connection_string, p_user=p_user, p_pwrd=p_password, p_https=p_https)
        return 1, None
    # create the string for selecting particular attributes
    l_select_string=""
    if p_attribute_list:
        if not p_table:
            AbaseError(p_error_text="A table should be mentioned while attribute is",p_module="OneC",p_class="",
                       p_def="get_data").raise_error()
        l_select_string=get_onec_select(p_attribute_list=p_attribute_list)
    # add table to the end of connection string
    l_connection_string+="/"+p_table
    # add data format to the end of connection string
    l_connection_string=l_connection_string+C_1C_DATA_DATA_FORMAT+l_select_string
    #get the data using OData
    l_response=OData.get_response(p_connection_string=l_connection_string, p_user=p_user, p_pwrd=p_password, p_https=p_https)
    l_data=json.loads(l_response)
    # if error, return error
    if l_data.get('odata.error'):
        l_error=l_data.get('odata.error').get('message').get('value')
        return l_query_output, l_error
    else:
        # transform the list of dicts into list of tuples
        l_data=l_data.get('value')
        for index, i_row in enumerate(l_data):
            l_transform_row=tuple(i_row.values())
            l_data[index]=l_transform_row
        return l_data, None

def get_onec_select(p_attribute_list: list) -> str:
    """
    Creates the string for selecting particular attributes from 1C

    :param p_attribute_list: list of attribute to get from 1C
    :return: string for selecting particular attributes
    """
    l_select_string=C_1C_SELECT_COMMAND
    for i_attr in p_attribute_list:
        l_select_string+=i_attr+','
    l_select_string=l_select_string[:-1]
    return l_select_string