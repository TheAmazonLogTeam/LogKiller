import re
import multiprocessing
import pandas as pd
import numpy as np
import time
import sys


def parse(log):
    """
    parse un log en liste de mots
    """
    log = log.replace("=", " ").replace(" ; ", " ").replace(" : ", " ")  # ajouter cas COMMAND=""
    log_split = re.compile('(?:[^\s(]|\([^)]*\))+').findall(log)

    if len(log_split) >= 4:
        if log_split[3] == '':
            return [None]
        else:
            return log_split
    else:
        return [None]

def parse_all_simple(logs):
    """
    parse une liste de logs en df de mots indexés par leur timestamp
    """

    ## -- Parsing -- ##

    logs_split = []
    for log in logs:
        log_split = log.split(' ', 3)
        if len(log_split) == 4:
            logs_split.append(log_split)

    ## -- Mise en dataframe -- ##

    parsed_log_df = pd.DataFrame(logs_split)
    parsed_log_df = parsed_log_df[~df.isnull().all(axis=1)]
    parsed_log_matrix = parsed_log_df.values

    indices = parsed_log_matrix[:, 0]  # timestamp
    hostnames = parsed_log_matrix[:, 1].reshape(-1, 1)  # hostname
    services = parsed_log_matrix[:, 2].reshape(-1, 1)  # service[PID]
    messages = parsed_log_matrix[:, 3].reshape(-1, 1)  # message

    data = np.concatenate((hostnames, services, messages), axis=1)
    df_log = pd.DataFrame(data=data, index=indices, columns=[
                          'hostname', 'service', 'message'])

    df_log = df_log[df_log.index.notnull()]

    return df_log


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

    try:
        indices = pd.Index(pd.to_datetime(
            parsed_log_matrix[:, 0], format='%Y-%m-%dT%H:%M:%S'), name='timestamp')  # timestamp
        hostnames = parsed_log_matrix[:, 1].reshape(-1, 1)  # hostname
        services = parsed_log_matrix[:, 2].reshape(-1, 1)  # service[PID]
        messages = parsed_log_matrix[:, 3:]  # message

        data = np.concatenate((hostnames, services, messages), axis=1)
        df_log = pd.DataFrame(data=data, index=indices)

    except (ValueError, TypeError):

        start = time.time()

        print("[Warning] Bad timestamp detected !")
        indices = parsed_log_matrix[:, 0]  # timestamp
        hostnames = parsed_log_matrix[:, 1].reshape(-1, 1)  # hostname
        services = parsed_log_matrix[:, 2].reshape(-1, 1)  # service[PID]
        messages = parsed_log_matrix[:, 3:]  # message

        data = np.concatenate((hostnames, services, messages), axis=1)
        df_log = pd.DataFrame(data=data, index=indices)

        good_indices = []
        for i, idx in enumerate(df_log.index):

            if i % int(len(df_log.index)/100) == 0:
                percentage = str(int(100*i/len(df_log.index))) + " %"
                print("[Info] Correction in progress ... " +
                      percentage, end="\r")

            try:
                good_indices.append(pd.to_datetime(
                    idx, format='%Y-%m-%dT%H:%M:%S'))
            except (ValueError, TypeError):
                good_indices.append(np.nan)

        df_log.index = good_indices

        df_log = df_log[df_log.index.notnull()]

        end = time.time()

        print('[Success] Timestamp corrected within %.2fs' % (end - start))

    return df_log
