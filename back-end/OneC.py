import OData
import json
from Constants import *
from SystemObjects import *

def get_data(
        p_server: str,
        p_db: str,
        p_table: str,
        p_attribute_list: list,
        p_user: str =None,
        p_pwrd: str =None,
        p_https: int =None
):
    """
    Get the data from 1C web-server

    :param p_server: 1C server
    :param p_db: information base 1C
    :param p_table: data catalog
    :param p_attribute_list: list of attributes to get from 1C
    :param p_user: 1C login which is used while connecting to information base
    :param p_pwrd: 1C password which is used while connecting to information base
    :return: tuple with data or error text
    """
    l_query_output=None
    l_error=None
    # create the connection string
    l_connection_string=p_server+"/"+p_db+"/"+C_1C_CONNECTION_STRING+"/"+p_table
    # create the string for selecting particular attributes
    l_select_string=get_onec_select(p_attribute_list=p_attribute_list)
    # add data format to the end of connection string
    l_connection_string=l_connection_string+l_select_string+C_1C_DATA_DATA_FORMAT
    #get the data using OData
    l_data=OData.get_response(p_connection_string=l_connection_string, p_user=p_user, p_pwrd=p_pwrd, p_https=p_https)
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
        return l_data

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