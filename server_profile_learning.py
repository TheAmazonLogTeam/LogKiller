import times_series_learning as tsl
import numpy as np
import time
import datetime as dt
import sortedcontainers
import pandas as pd


class ServerProfileLearning(object):

    def __init__(self, data, parameters, distribution, distribution_period, level_threshold):
        self.label_number = len(np.unique(data['label'].values))
        self.label = np.unique(data['label'].values)
        self.data = data
        self.parameters = np.ones((self.label_number + 1, 4)) * parameters  # see parameters in times_series_learning
        self.data_prep = None
        self.hostname = self.data.iloc[0, 1]
        self.server_profile = dict()
        self.distribution = distribution  # distribution od distance list same for all servers all clusters be carefull sorted containers
        self.distribution_period = distribution_period  # distribution period where we aggregate distance score
        self.level_threshold = level_threshold  # level we consider for outliers

    # sortedcontainers.SortedDict(sortedcontainers.SortedList())

    def preprocess_data(self, data):
        data_prep = data.drop(self.data.columns[1:len(self.data.columns) - 1], axis=1)
        data_prep = data_prep.groupby(['label'])
        return data_prep

    def set_profile(self):
        t0 = time.time()
        self.data_prep = self.preprocess_data(self.data)
        t = tsl.TimesSeriesLearning(self.parameters[0, :],
                                    self.distribution_period, self.distribution, self.level_threshold)
        t.set_profile(self.data)
        self.server_profile[self.hostname + "_general"] = t
        i = 0

        for k, v in self.data_prep:
            t = tsl.TimesSeriesLearning(self.parameters[i, :],
                                        self.distribution_period, self.distribution, self.level_threshold)
            t.set_profile(v)
            self.server_profile[self.hostname + "_" + str(k)] = t
            print('cluster number ' + str(k) + ' of hostname: ' + self.hostname)
            i += 1
        print("Learning Server" + self.hostname + " Done in " + str(time.time() - t0))

    # Process distance and update distribution
    def process_distance(self, streaming_data):
        t0 = time.time()
        streaming_data_prep = self.preprocess_data(streaming_data)
        cluster_name = self.hostname + "_general"
        t = self.server_profile[cluster_name]
        anomaly, max_spread, min_spread, d, date, threshold, quant = t.compute_distance_profile(streaming_data)
        for k, v in streaming_data_prep:
            cluster_name = self.hostname + "_" + str(k)
            if cluster_name in self.server_profile.keys():
                t = self.server_profile[cluster_name]
                anomaly, max_spread, min_spread, d, date, threshold, quant = t.compute_distance_profile(v)
                if anomaly:
                    break
            else:
                print('cluster: ',k)
                print("Logs does not belong to any cluster")
                break
        print("stream proccessed in :", time.time()-t0)
        return anomaly, max_spread, min_spread, d, date, threshold, quant

    def simulate_streaming(self, streaming_data):
        streaming_data.index = pd.to_datetime(streaming_data.timestamp, format='%Y-%m-%d %H:%M:%S')
        data_list = []
        date = streaming_data.index[0]
        while date < streaming_data.index[-1]:
            data_list.append(streaming_data.loc[date.isoformat():
                                                (date + dt.timedelta(
                                                    minutes=self.parameters[2, 0])).isoformat()].reset_index(drop=True))
            date += dt.timedelta(minutes=self.parameters[0, 2])

        return data_list
