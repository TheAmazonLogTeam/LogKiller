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
    analyzer = Analyzer()

    logs = reader.read_dir('../logs/*',100)

    parsed_logs = parser.parse_all(logs)

    sorted_logs = parser.sort(parsed_logs)

    connection_logs_df = pd.DataFrame(sorted_logs['Connection'])
    command_logs_df = pd.DataFrame(sorted_logs['Command'])

    analyzer.import_to_neo4j(connection_logs_df, command_logs_df)