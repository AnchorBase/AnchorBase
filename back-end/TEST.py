import SystemObjects
from SystemObjects import Error
try:
    Error(p_error_text="xxx",p_module="DWH",p_class="" ,p_def="create_ddl").raise_error()
except Exception as e:
    print(e)
