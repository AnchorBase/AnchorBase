import requests
from requests.auth import HTTPBasicAuth
import json
from Constants import *
from SystemObjects import *


def get_data(
        p_connection_string,
        p_user: str =None,
        p_pwrd: str =None
) -> dict:
    """
    Get data from web using http

    :param p_connection_string: connection string
    :param p_user: login
    :param p_pwrd: password
    :return : dict with values
    """
    # login and password should be in UTF-8
    l_auth=None
    if p_user or p_pwrd:
        p_user=p_user.encode('utf-8')
        if p_pwrd:
            p_pwrd=p_pwrd.encode('utf-8')
        l_auth = HTTPBasicAuth(p_user, p_pwrd)
    # add data format to the end of connection string
    p_connection_string=p_connection_string+C_ODATA_DATA_FORMAT
    # get data from server
    l_response=requests.get(p_connection_string,auth=l_auth)
    # if the 'text' property is none - it's an error
    if not l_response.text:
        AbaseError(p_error_text="The error occurred: Code - "+str(l_response.status_code)+" reason - "+str(l_response.reason),p_module="OData",p_class="",
                   p_def="dbms_type").raise_error()
    # transform result from json into dictionary
    l_data=json.loads(l_response.text)
    return l_data

