import requests
from requests.auth import HTTPBasicAuth
from SystemObjects import *


def get_response(
        p_connection_string,
        p_user: str =None,
        p_pwrd: str =None,
        p_https: int =0
) -> tuple:
    """
    Get the data from web using http

    :param p_connection_string: connection string (without HTTP|HTTPS://)
    :param p_user: login
    :param p_pwrd: password
    ;param p_https: 1 - HTTPS, 0 - HTTP
    :return : tuple(dict with values, error)
    """
    l_error=None
    try:
        l_http="https" if p_https==1 else "http"
        # login and password should be in UTF-8
        l_auth=None
        if p_user or p_pwrd:
            p_user=p_user.encode('utf-8')
            if p_pwrd:
                p_pwrd=p_pwrd.encode('utf-8')
            l_auth = HTTPBasicAuth(p_user, p_pwrd)
        # add http/https
        l_connection_string=l_http+"://"+p_connection_string
        # get data from server
        l_response=requests.get(l_connection_string,auth=l_auth, timeout=(10, None))
        # if the 'text' property is none - it's an error
        if not l_response.text:
            l_error="The error occurred: Code - "+str(l_response.status_code)+" reason - "+str(l_response.reason)
            return None, l_error
        return l_response.text, None
    except Exception as e:
        return None, e.args[0].args[0]

