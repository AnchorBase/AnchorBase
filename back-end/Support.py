import Metadata

#Класс вспомогательных функций
class Support:
    # вывод ошибок
    @staticmethod
    def error_output(cls, func, text):
        error = {"class":cls,"function":func,"text":text}
        return error