import requests
from requests.auth import HTTPBasicAuth
import json
from Constants import *
from SystemObjects import *
import sys


def get_response(
        p_connection_string,
        p_user: str =None,
        p_pwrd: str =None,
        p_https: int =0
) -> dict:
    """
    Get the data from web using http

    :param p_connection_string: connection string (without HTTP|HTTPS://)
    :param p_user: login
    :param p_pwrd: password
    ;param p_https: 1 - HTTPS, 0 - HTTP
    :return : dict with values
    """
    l_http="https" if p_https==1 else "http"
    # login and password should be in UTF-8
    l_auth=None
    if p_user or p_pwrd:
        p_user=p_user.encode('utf-8')
        if p_pwrd:
            p_pwrd=p_pwrd.encode('utf-8')
        l_auth = HTTPBasicAuth(p_user, p_pwrd)
    # add http/https
    p_connection_string=l_http+"://"+p_connection_string
    # get data from server
    l_response=requests.get(p_connection_string,auth=l_auth)
    # if the 'text' property is none - it's an error
    if not l_response.text:
        AbaseError(p_error_text="The error occurred: Code - "+str(l_response.status_code)+" reason - "+str(l_response.reason),p_module="OData",p_class="",
                   p_def="dbms_type").raise_error()
    # transform result from json into dictionary
    l_data=json.loads(l_response.text)
    return l_data

