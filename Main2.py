import pandas as pd
from Reader import Reader
from Parser import Parser
from Analyzer import Analyzer
import time
import multiprocessing
from cassandra.cluster import Cluster

pool = multiprocessing.Pool()

if __name__ == '__main__':

    reader = Reader()
    parser = Parser()
    analyzer = Analyzer()

    # Gathering all logs in one list
    logs = reader.read_dir('../logs/*',10)
    start = time.time()
    parsed_logs = list(pool.map(parser.parse,logs))
    end = time.time()
    print('[Sucess] Logs parsed within %.2f s' % (end-start))

    # add to cassandra column-oriented
    # query ip and command log to fill neo4j