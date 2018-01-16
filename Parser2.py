import time
from pyparsing import *


class Parser(object):
    """LogParser"""

    def __init__(self):
        """
        Parser initialisation of patterns
        """

        timestamp = Word(nums + 'T' + '-' + ':' + '+')

        hostname = Word(alphas)
        file_nb = Word(nums)

        appname = Word(alphas)
        pid = Suppress("[") + Word(nums) + Suppress("]") + Suppress(":")

        sudo = Word(alphas) + Suppress(":")

        message = Regex(".*")

        user = Suppress("(") + Word(alphas) + Suppress(")")
        command = Word(alphas.upper())
        arg = Suppress("(") + Word(alphas + "/" +
                                   "." + "-" + " ") + Suppress(")")

        ip = Combine(Word(nums, max=3) + "." + Word(nums, max=3) + "." +
                     Word(nums, max=3) + "." + Word(nums, max=3))

        pattern_1 = timestamp + hostname + file_nb + appname + pid + SkipTo(ip, include=True) + message
        pattern_2 = timestamp + hostname + file_nb + appname + pid + user + command + arg
        pattern_3 = timestamp + hostname + file_nb + appname + pid + message
        pattern_4 = timestamp + hostname + file_nb + sudo + message
        pattern_5 = timestamp + hostname + file_nb + message

        patterns = [pattern_1, pattern_2, pattern_3, pattern_4, pattern_5]
        fields = [['time', 'hostname', 'file_nb', 'appname', 'pid', 'message1', 'ip', 'message2'],
                  ['time', 'hostname', 'file_nb', 'appname', 'pid', 'user', 'command', 'arg'],
                  ['time', 'hostname', 'file_nb', 'appname', 'pid', 'message'],
                  ['time', 'hostname', 'file_nb', 'sudo', 'message'],
                  ['time', 'hostname', 'file_nb', 'message']]

        self.patterns_ = patterns
        self.fields_ = fields

    def parse(self, log):
        """
        Log parsing method
        """
        for n, pattern in enumerate(self.patterns_):
            try:
                parsed_log = pattern.parseString(log)
                parsed_dict = dict(zip(self.fields_[n], parsed_log))
                break
            except:
                pass

        return parsed_dict
