"""
Системные объекты (Константы, ошибки и т.д.)
"""

class AbaseError(Exception):

    """
      Errors
    """

    def __init__(self, p_error_text: str, p_module: str, p_class: str, p_def: str):
        """

        @param p_error_text: text of error
        @param p_module: module, where error occured
        @param p_class: class, where error occured (if class exists)
        @param p_def: function, where error occured
        """
        self._error_text=p_error_text
        self._module=p_module
        self._class=p_class
        self._def=p_def


    @property
    def error_text(self):
        """
        Text of errors
        """
        # checks
        if self._error_text.__len__()>0:
            return self._error_text
#returns hash value of module, class and function
    @property
    def error_code(self):
        return str(hash(self._module))+'.'+str(hash(self._class))+'.'+str(hash(self._def))

    def raise_error(self):
        """
        Raises error message, that have been given in p_error_text param
        """
        l_arg=(self.error_code+'\n'+self.error_text, None)
        self.args=l_arg
        raise self


