import times_series_learning as tsl
import numpy as np
import pandas as pd

class ServerProfileLearning(object):

    def __init__(self, data, parameters):
        self.label_number = len(np.unique(data['label'].values))
        self.label = np.unique(data['label'].values)
        self.data = data
        self.parameters = np.ones((self.label_number+1, 4))*parameters
        self.data_prep = None
        self.hostname = self.data.iloc[1, 0]
        self.server_profile= dict()

    def preprocess_data(self):
        self.data_prep = self.data.drop(self.data.columns[1:len(self.data.columns) - 1], axis=1)
        self.data_prep = self.data_prep.groupby(['label'])

    def set_profile(self):

        self.preprocess_data()
        t = tsl.TimesSeriesLearning(self.parameters[0, :])
        t.set_profile(t.weekly_average(t.mov_avg(t.get_time_series_rs(self.data['timestamp']))))
        self.server_profile[self.hostname + "_general"]=t
        i=0

        for k, v in self.data_prep:
            t = tsl.TimesSeriesLearning(self.parameters[i, :])
            t.set_profile(v)
            self.server_profile[self.hostname + "_"+str(k)] = t
            i += 1



