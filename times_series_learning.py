import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import numpy as np
from scipy.spatial.distance import euclidean
from fastdtw import fastdtw
from datetime import datetime
import time


# Class that build a weekly profil type
# Compare streaming logs to profil type
# If no mistakes update the profil type

class TimesSeriesLearning(object):

    def __init__(self, parameters):
        self.learning_week_period = 0
        self.period = parameters[0]
        self.m_avg_period = parameters[1]
        self.dist_period = parameters[2]
        self.dist_radius = parameters[3]
        self.data_week_avg = None
        self.streaming_start_date = None
        self.start = False
        self.level_threshold = 0
        self.profile = None

    # Resample
    def get_time_series_rs(self, data):
        data.index = pd.to_datetime(data.timestamp, format='%Y-%m-%d %H:%M:%S')
        d_min = data.drop(data.columns[1:], axis=1)
        d_min = d_min.resample(self.period).count()
        return d_min

    # Moving average
    def mov_avg(self, data):
        return data.rolling(window=self.m_avg_period).mean().fillna(method='backfill')

    # Dataframe for grouping per week
    def weekly_average(self, data):
        data['minutes'] = data.index.minute
        data['hour'] = data.index.hour
        data["weekday"] = data.index.weekday
        data["weekday_name"] = data.index.weekday_name
        return data.groupby(['weekday_name', 'hour', 'minutes']).mean()['timestamp']

    # Set the profile

    def set_profile(self, data):
        delta = (data.loc[-1, 'timestamp'] - data.loc[0, 'timestamp']).week
        self.learning_week_period = delta
        self.profile = self.weekly_average(self.mov_avg(self.get_time_series_rs(data)))

    # Online Mean
    # contiguous data
    def compute_mean(self, streaming_data):
        if not self.start:
            self.streaming_start_date = streaming_data.index[0]
        delta = (streaming_data.inex[0] - self.streaming_start_date).week
        self.week_number += delta
        minute = streaming_data.loc[0, 'minutes']
        last_minute = streaming_data.loc[-1, 'minutes']
        hour = streaming_data.loc[0, 'hours']
        weekday = streaming_data.loc[0, 'weekday']
        self.profile_type.loc[weekday][hour][minute: last_minute].values = self.profile_type.loc[weekday][hour][
                                                                           minute: last_minute].values \
                                                                           + (streaming_data -
                                                                              self.profile_type.loc[weekday][hour][
                                                                              minute: last_minute].values) / \
                                                                           self.learning_week_period

    # compute distance the size of the window of the streaming batch should be
    # arrange before calling this function
    def compute_distance(self, streaming_data, profile_type):

        minute = streaming_data.loc[0, 'minutes']
        hour = streaming_data.loc[0, 'hours']
        weekday = streaming_data.loc[0, 'weekday']

        d = fastdtw(profile_type.loc[weekday][hour][minute:minute + self.dist_period].values,
                    streaming_data[weekday][hour][minute:minute + self.dist_period].values,
                    self.dist_radius, dist=euclidean)[0]
        return d

    def threshold(self, d, streaming_data):
        if d >= self.level_threshold:
            return streaming_data
        else:
            self.compute_mean(self, streaming_data)
