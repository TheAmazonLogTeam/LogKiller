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

    def parse(log):
        return [log.split(' ',3)[0],log.split(' ',3)[1],log.split(' ',3)[3]]

    print(parse(logs[0]))