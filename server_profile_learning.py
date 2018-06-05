import times_series_learning as tsl
import numpy as np
import time
import datetime as dt
import sortedcontainers
import pandas as pd
from collections import defaultdict


class ServerProfileLearning(object):

    def __init__(self, data, parameters, distribution, distribution_period, level_threshold,
                 processus=True, moving_window=60,train_mode=True, verbose=False):
        self.label_number = len(np.unique(data['label'].values))
        self.label = np.unique(data['label'].values)
        self.data = data
        self.parameters = np.ones((self.label_number + 1, 4)) * parameters  # see parameters in times_series_learning
        self.data_prep = None
        self.hostname = self.data.iloc[0, 1]
        self.server_profile = dict()
        self.distribution = distribution  # distribution of distance list same for all servers all clusters be carefull sorted containers
        self.distribution_period = distribution_period  # distribution period where we compute metrics
        self.level_threshold = level_threshold  # level we consider for outliers
        self.verbose = verbose
        self.processus = processus
        self.moving_window = moving_window
        self.train_mode = train_mode
        self.measures = self.initdict()

    def initdict(self):
        d = defaultdict(dict)
        for i in range(int(self.distribution_period/(24*6*60))):
            d[i] = {}
            d[i]['Area_Difference'] = []
            d[i]['Max_Spread'] = []
        return d


    # sortedcontainers.SortedDict(sortedcontainers.SortedList())

    def preprocess_data(self, data):
        data_prep = data.drop(self.data.columns[1:len(self.data.columns) - 1], axis=1)
        data_prep = data_prep.groupby(['label'])
        return data_prep

    def set_profile(self):
        t0 = time.time()
        t = tsl.TimesSeriesLearning(self.parameters[0, :],
                                    self.distribution_period, self.level_threshold, self.processus)
        t.set_profile(self.data)
        self.server_profile[self.hostname + "_general"] = t
        self.data_prep = self.preprocess_data(self.data)
        i = 0
        for k, v in self.data_prep:
            t = tsl.TimesSeriesLearning(self.parameters[i, :],
                                        self.distribution_period, self.level_threshold, self.processus)
            t.set_profile(v)
            self.server_profile[self.hostname + "_" + str(k)] = t
            print('cluster number ' + str(k) + ' of hostname: ' + self.hostname)
            i += 1
        print("Learning Server" + self.hostname + " Done in " + str(time.time() - t0))

    # Process distance and update distribution
    def process_distance(self, streaming_data):
        t0 = time.time()
        cluster_name = self.hostname + "_general"
        t = self.server_profile[cluster_name]
        anomaly, max_spread, min_spread, d, date, threshold, quant = t.compute_distance_profile(streaming_data,
                                                                                                self.distribution,
                                                                                                self.measures,
                                                                                                self.train_mode,
                                                                                                self.verbose)
        #streaming_data_prep = self.preprocess_data(streaming_data)
        # for k, v in streaming_data_prep:
        #     cluster_name = self.hostname + "_" + str(k)
        #     if cluster_name in self.server_profile.keys():
        #         t = self.server_profile[cluster_name]
        #         anomaly, max_spread, min_spread, d, date, threshold, quant = t.compute_distance_profile(v,
        #                                                                                                 self.distribution,
        #                                                                                                 self.train_mode,
        #                                                                                                 self.verbose)
        #         #if anomaly:
        #             # break
        #     else:
        #         print('cluster: ',k)
        #         print("Logs does not belong to any cluster")
        #         break
        #print("stream proccessed in :", time.time()-t0)
        return anomaly, max_spread, min_spread, d, date, threshold, quant

    # def simulate_streaming(self, streaming_data,date_start):
    #     streaming_data.index = pd.to_datetime(streaming_data.timestamp, format='%Y-%m-%d %H:%M:%S')
    #     streaming_data = streaming_data.sort_index()
    #     data_list = []
    #     date = streaming_data.index[0]
    #     while date < streaming_data.index[-1]:
    #         data_to_add = streaming_data.loc[date.isoformat():
    #                                          (date + dt.timedelta(minutes=self.parameters[2, 0]))].reset_index(drop=True)
    #         if data_to_add.shape[0]>0:
    #             data_list.append(data_to_add)
    #         date += dt.timedelta(minutes=self.parameters[0, 2])
    #
    #     return data
