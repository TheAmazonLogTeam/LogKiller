import pandas as pd
from Reader import Reader
from Parser2 import Parser
from Analyzer import Analyzer
import time
from cassandra.cluster import Cluster
import pprint

if __name__ == '__main__':

    reader = Reader()
    parser = Parser()

    logs = reader.read_dir('../logs/*',10)

    parsed_logs = parser.parse_all(logs)
    sorted_logs = parser.sort(parsed_logs)

    pprint.pprint(sorted_logs['Unknown'][:10])