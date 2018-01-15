from glob import glob
import os.path as op
import csv
import re
import pandas as pd
from Reader import Reader
from Parser import Parser
from py2neo import Graph, Node
import numpy as np
import time


class Analyzer:

    def get_dict(self, df, key, value):
        group_df = df.groupby(key)
        dictionnary = {}
        for key_, value_ in group_df:
            value_ = value_[value]
            value_['count'] = 1
            value_ = value_.groupby(value).count()[['count']]
            dictionnary[key_] = value_
        return dictionnary

    def print_dict(self, dictionnary):
        for key in dictionnary.keys():
            print("\n*" + str(key))
            print(dictionnary[key])
    pd.
    # simple test, à améliorer
    def import_to_neo4j(self, connection_logs_df, command_logs_df):
        start = time.time()

        graph = Graph('http://localhost:7474')

        graph.delete_all()

        tx = graph.begin()

    
        hostname_set = set(connection_logs_df['hostname']) | set(
            command_logs_df['hostname'])
        ip_set = set(connection_logs_df['ip'])
        user_set = set(command_logs_df['user'])

        for hostname in hostname_set:
            h = Node("Hostname", id=hostname)
            tx.create(h)

        for ip in ip_set:
            i = Node("IP", id=ip)
            tx.create(i)

        for user in user_set:
            u = Node("User", id=user)
            tx.create(u)

        # Connexions IP
        for hostname, df_select in connection_logs_df.groupby('hostname'):
            for ip, count in df_select.groupby('ip'):
                # count = int(np.mean(count.values))
                tx.run("MATCH (h:Hostname {id:\"" + hostname + "\"}) MATCH (i:IP {id:\"" +
                       ip + "\"}) CREATE (h)-[:CALLED_BY{count:\"" + str(1) + "\"}]->(i)")

        # Commandes Bash
        for hostname, df_hostname in command_logs_df.groupby('hostname'):
            for user, df_user in df_hostname.groupby('user'):
                for command, df_command in df_user.groupby(['command','arg']):
                    arg = command[1].replace("\"","").replace(";","").replace("\\","")
                    command = command[0]
                    count = len(df_command)
                    tx.run("MATCH (h:Hostname {id:\"" + hostname + "\"}) MATCH (u:User {id:\"" +
                           user + "\"}) CREATE (u)-[:" + command + "{arg:\"" + arg + "\",count:\"" +str(count) + "\"}]->(h)")



        tx.commit()
        end = time.time()
        print("\n[Sucess] Data stored into Neo4j within %.2fs" % (end-start))

