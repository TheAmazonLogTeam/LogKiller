import pandas as pd
from Reader import Reader
from Parser2 import Parser
from Analyzer import Analyzer
import time
from cassandra.cluster import Cluster
import pprint
from py2neo import Graph, Node


if __name__ == '__main__':

    reader = Reader()
    parser = Parser()
    analyzer = Analyzer()

    logs = reader.read_dir('../log2/*')

    parsed_logs = parser.parse_all(logs)

    sorted_logs = parser.sort(parsed_logs)

    connection_logs_df = pd.DataFrame(sorted_logs['Connection'])
    command_logs_df = pd.DataFrame(sorted_logs['Command'])

    #Standard_1_logs_df = pd.DataFrame(sorted_logs['Standard_1'])
    #Standard_2_logs_df = pd.DataFrame(sorted_logs['Standard_2'])
    #Standard_3_logs_df = pd.DataFrame(sorted_logs['Standard_3'])
    #Standard_1_logs_df.to_csv('standard_1.csv')
    #Standard_2_logs_df.to_csv('standard_2.csv')
    #Standard_3_logs_df.to_csv('standard_3.csv')

    analyzer.import_to_neo4j(connection_logs_df, command_logs_df)
