# coding=utf-8

class TitleOverTimeException(Exception):
    def __init__(self, value=u"数据时间太过久远"):
        self.value = value

    def __str__(self):
        return repr(self.value)
class StatusCodeException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value