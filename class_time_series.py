import pandas as pd
import re
import glob
from datetime import datetime
import numpy as np
import collections



def makehash():
    return collections.defaultdict(makehash)


class GetServerData(object):
    """ Analyse the time series by serveur and by Weeks, Days, Hours, Minutes"""

    def __init__(self, LogPath):
        self.LogPath = LogPath
        self.ServerDict = collections.defaultdict(dict)
        self.FileDf = None



    # def getServerLogs(self, serverlogpath):
    #     # argument list of path given by self.FileDf
    #
    #     reader = r.Reader()
    #     serverlogs = reader.read_file_list(serverlogpath)
    #     return serverlogs
    #
    # def getTimeSeriesServer(self, serverlogpath):
    #     """to do """
    #     k = p.Parser()
    #     logs = self.getServerLogs(serverlogpath)
    #     print(logs)
    #     parsed_logs = k.parse_all(logs)
    #
    #     start = time.time()
    #
    #     data = pd.DataFrame(parsed_logs)
    #     data.sort_values('time')
    #
    #     times = pd.to_datetime(data.time)
    #     data['time'] = times
    #
    #     minutes_day = data.groupby([times.dt.hour, times.dt.minute]).sum()
    #     minutes_week = data.groupby([times.dt.weekday, times.dt.hour, times.dt.minute]).sum()
    #
    #     end = time.time()
    #     print("GroupBy days, hours, minutes done in %.2f s" % (end - start))
    #
    #     return minutes_day, minutes_week
    #
    # def getTimeSeriesServerRs(self, serverlogpath):
    #     """to do """
    #     #print(serverlogpath)
    #     k = p.Parser()
    #     logs = self.getServerLogs(serverlogpath)
    #     parsed_logs = k.parse_all(logs)
    #     data = pd.DataFrame(parsed_logs)
    #     data.index = pd.to_datetime(data.time, format='%Y-%m-%d %H:%M:%S')
    #     d_min = data.resample('min').count()
    #     return d_min
    #
    # def getTimeSeriesRs(data):
    #     """to do """
    #     #print(serverlogpath)
    #     #k = p.Parser()
    #     #logs = self.getServerLogs(serverlogpath)
    #     #parsed_logs = k.parse_all(logs)
    #     #data = pd.DataFrame(parsed_logs)
    #     data.index = pd.to_datetime(data.time, format='%Y-%m-%d %H:%M:%S')
    #     d_min = data.resample('min').count()
    #     return d_min
    #
    #



    #def plotTimeSeriesLogs(self, time_series_data, duration="hours"):

     #   plt.figure(figsize=(15, 10))

     #   if duration == "hours":
     #       plt.plot(range(time_series_data.shape[0]), time_series_data, label="count logs / hour")
     #       plt.legend()
     #       plt.xlabel("Hours")

     #   elif duration == "days":
     #       plt.plot(range(time_series_data.shape[0]), time_series_data, label="count logs / day")
     #       plt.legend()
     #      plt.xlabel("Days")

     #    elif duration == "minutes":
     #       plt.plot(range(time_series_data.shape[0]), time_series_data, label="count logs / minute")
     #       plt.legend()
     #       plt.xlabel("Minutes")

     #   else:
     #       print("not a valid duration")

    #    plt.ylabel("Number of Logs")
     #   plt.show()


    def ServerList(self):
        # regexp link
        # https://regex101.com/r/ms9q87/1
        # dataframe
        # regexp = r"([a-z]{10}\d{2}).*|([a-z]{7})\d.*|(machine\.[a-z]+)|(machine)\.\d+|(leader1\.[a-z]+)|(\w+)\.d*"
        regexp2 = r"(\d+\.\d+\.\d+\.\d+)\.\d+|(\w*)\.\d+|(\w*\.\w*)\.\d+"
        regexp3 = r"\.(\d+)\.log"
        FileList = glob.glob(self.LogPath + '/*.log')
        h = 0
        k = 0

        for i, j in enumerate(FileList):
            # c = re.search(regexp, j).groups()
            d = re.search(regexp2, j).groups()
            e = re.search(regexp3,j).groups()
            date=[]

            if e[0] is not None:
                date = datetime(year=int(str(e[0])[:4]), month=int(str(e[0])[4:6]), day=int(str(e[0])[6:]))
            # h = np.argwhere(np.asanyarray(c) != None).ravel()[0]
            k = np.argwhere(np.asanyarray(d) != None).ravel()[0]

            self.ServerDict[d[k]][date] =j

        self.FileDf = pd.DataFrame(data=self.ServerDict).fillna(value=0)
        #print(self.FileDf.head(15))
        #print(str(i) + " files to server mapped")
        return self.FileDf

