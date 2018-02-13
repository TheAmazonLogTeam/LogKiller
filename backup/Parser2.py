import time
from pyparsing import *
import multiprocessing
from tqdm import tqdm


class Parser(object):
    """LogParser"""

    def __init__(self):

        """
        Parser initialisation of patterns
        """
        timestamp = Word(nums + 'T' + '-' + ':' + '+')

        hostname = Word(alphas)
        file_nb = Word(nums)

        appname = SkipTo("[") + Suppress('[')
        pid = Word(nums) + Suppress("]:")

        sudo = SkipTo(":") + Suppress(":")

        message = Regex(".*")

        user = Suppress("(") + Word(alphas) + Suppress(")")
        command = Word(alphas.upper())
        arg = Suppress("(") + SkipTo(")") + Suppress(")")

        ip = Combine(Word(nums, max=3) + "." + Word(nums, max=3) + "." +
                     Word(nums, max=3) + "." + Word(nums, max=3))

        pattern_0 = timestamp + hostname + file_nb + \
            appname + pid + SkipTo(ip, include=True) + message
        pattern_1 = timestamp + hostname + file_nb + \
            appname + pid + user + command + arg
        pattern_2 = timestamp + hostname + file_nb + appname + pid + message
        pattern_3 = timestamp + hostname + file_nb + sudo + message
        pattern_4 = timestamp + hostname + file_nb + message

        patterns = [pattern_0, pattern_1, pattern_2, pattern_3, pattern_4]
        pattern_names = ['Connection', 'Command', 'Standard_1', 'Standard_2', 'Standard_3']
        fields = [['time', 'hostname', 'file_nb', 'appname', 'pid', 'message1', 'ip', 'message2', 'type'],
                  ['time', 'hostname', 'file_nb', 'appname',
                      'pid', 'user', 'command', 'arg', 'type'],
                  ['time', 'hostname', 'file_nb',
                      'appname', 'pid', 'message', 'type'],
                  ['time', 'hostname', 'file_nb', 'sudo', 'message', 'type'],
                  ['time', 'hostname', 'file_nb', 'message', 'type']]

        self.patterns_ = patterns
        self.pattern_names_ = pattern_names
        self.fields_ = fields

    def parse(self, log):
        """
        Log parsing method
        """
        parsed_dict = {}
        for n, pattern in enumerate(self.patterns_):
            try:
                parsed_log = pattern.parseString(log)
                parsed_log.append(n)
                parsed_dict = dict(zip(self.fields_[n], parsed_log))
                break
            except:
                pass

        return parsed_dict

    def parse_all(self, logs):
        """
        Parse all logs
        """
        start = time.time()
        pool = multiprocessing.Pool()

        #max_ = len(logs)
        #for _ in tqdm(pool.imap_unordered(self.parse, logs), total=max_):
        #    pass
        #pool.close()
        #pool.join()

        parsed_logs = list(pool.map(self.parse,logs))
        pool.close()
        pool.join()
        
        end = time.time()
        print('[Sucess] Logs parsed within %.2f s' % (end-start))

        return parsed_logs

    def sort(self, parsed_logs):
        """
        Parse logs according to 'type'
        """

        sorted_logs = { self.pattern_names_[n] : [parsed_log for parsed_log in parsed_logs if parsed_log['type'] == n]
            for n in range(len(self.patterns_))}

        for k,v in sorted_logs.items():
            print("\t - %s : %d (%.2f %%) " % (k,len(v),len(v)/len(parsed_logs)*100))

        return sorted_logs