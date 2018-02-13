import time
import multiprocessing

class Parser(object):
    """LogParser"""

    #def __init__(self):

    def parse_pattern(self, logs, pattern = 'all'):
        pass
        """
        Log -> Timestamp - Hostname - Service - PID? - Message*pattern
        """

    def parse(self, log):
        """
        Log -> Date - Hostname - Message
        """
        split_log = log.split(' ', 3)
        parsed_log = [ split_log[0][:10], split_log[1], split_log[3] ]

        return parsed_log

    def parse_all(self, logs):
        """
        Parse all logs
        """
        start = time.time()
        pool = multiprocessing.Pool()

        parsed_logs = list(pool.map(self.parse,logs))

        pool.close()
        pool.join()
        
        end = time.time()
        print('[Success] Logs parsed within %.2fs *parallelized*' % (end-start))

        return parsed_logs