import pandas as pd
from Reader import Reader
from Parser import Parser
from Analyzer import Analyzer


if __name__ == '__main__':

    reader = Reader()
    parser = Parser()
    analyzer = Analyzer()

    # Gathering all logs in one list
    logs = reader.read_dir('../logs/*',10)

    # Parsing each log into a list of string
    connection_logs, command_logs, standard_logs_1, standard_logs_2, unknown_logs = parser.parse_all_logs(logs)

    # Converting result to Dataframe
    connection_logs_df = pd.DataFrame(data=connection_logs, columns=['date','hostname','protocol','pid','ip','message'])
    command_logs_df = pd.DataFrame(data=command_logs, columns=['date','hostname','protocol','pid','user','command','arg'])
    standard_logs_1_df = pd.DataFrame(data=standard_logs_1, columns=['date','hostname','protocol','pid','message'])
    standard_logs_2_df = pd.DataFrame(data=standard_logs_2, columns=['date','hostname','protocol','message'])
    unknown_logs_df = pd.DataFrame(data=unknown_logs, columns=['date','hostname','message'])

    # Importing connections in Neo4j
    analyzer.import_to_neo4j(connection_logs_df, command_logs_df)

    # Getting dictionnary of commands
    #dict_1 = analyzer.get_dict(command_logs_df, ['hostname','user'], ['protocol','command','arg'])
    #dict_2 = analyzer.get_dict(command_logs_df, ['protocol', 'command', 'arg'],['hostname', 'user'])
    #analyzer.print_dict(dict_1)
    #analyzer.print_dict(dict_2)
