import pandas as pd
from Reader import Reader
from Parser2 import Parser
from Analyzer import Analyzer
import time
import pprint
from py2neo import Graph, Node


if __name__ == '__main__':

    reader = Reader()
    parser = Parser()
    analyzer = Analyzer()

    logs = reader.read_dir('../log/*')

    parsed_logs = parser.parse_all(logs)

    sorted_logs = parser.sort(parsed_logs)

    command_logs_df = pd.DataFrame(sorted_logs['Command'])
    print(command_logs_df[['appname','user','command','arg']])