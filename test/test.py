class TestClass:

    def __init__(self, number):
        self.__number = number

    def get_num(self):
        return self.__number

    def set_num(self, num):
        self.__number = num

    def __str__(self):
        return self.__number


class Profile:

    def __init__(self, name, address):
        self.__name = name
        self.__address = address

    def get_profile(self):
        return self.__name + " " + self.__address

    def __str__(self):
        return self.__name+" "+self.__address
