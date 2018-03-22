import re
import multiprocessing
import pandas as pd
import numpy as np
import time

def parse(log):
    """
    parse un log en liste de mots
    """
    log = log.replace("=", " ").replace(" ; ", " ").replace(" : ", " ") # ajouter cas COMMAND=""
    log_split = re.compile('(?:[^\s(]|\([^)]*\))+').findall(log)
    
    if log_split[2] == "--MARK--":
        log_split[2] = ""
        log_split.append("--MARK--")
        
    return log_split

def parse_all(logs):
    """
    parse une liste de logs en df de mots indexés par leur timestamp
    """
    
    ## -- Parsing -- ##
    
    pool = multiprocessing.Pool()

    start = time.time()
    parsed_logs = pool.map(parse, logs)
    end = time.time()

    print('[Success] Logs parsed within %.2fs' % (end - start))

    pool.close()
    pool.join()
    
    ## -- Mise en dataframe -- ##
    
    parsed_log_matrix = pd.DataFrame(parsed_logs).values
    
    indices = pd.Index(pd.to_datetime(
    parsed_log_matrix[:, 0], format='%Y-%m-%dT%H:%M:%S'), name='timestamp') # timestamp
    hostnames = parsed_log_matrix[:, 1].reshape(-1,1) # hostname
    services = parsed_log_matrix[:, 2].reshape(-1,1) # service[PID]
    messages = parsed_log_matrix[:, 3:] # message
    
    data = np.concatenate((hostnames, services, messages),axis=1)
    df_log = pd.DataFrame(data = data, index = indices)

    return df_log