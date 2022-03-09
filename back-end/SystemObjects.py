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
    @property
    def error_code(self):
        """
        Hash value of module, class and function
        """
        return str(self._module)+'.'+str(self._class)+'.'+str(self._def)

    def raise_error(self):
        """
        Raises error message
        """
        l_arg=(self.error_text+'\n\n'+self.error_code, None)
        self.args=l_arg
        raise self


