import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.spatial.distance import euclidean
from fastdtw import fastdtw
from scipy.stats.mstats import mquantiles
from collections import defaultdict
from datetime import datetime
import time

# distribution of distances by time step (type day)
# update if it is not in distribution skew
# Danger profile on te learning times series do not do  an average ideally
# Class that build a weekly profile type
# Compare streaming logs to profile type
# If no mistakes update the profile type
# compute the spread between two logs


class TimesSeriesLearning (object):

    def __init__(self, parameters, distribution_period, distribution, level_threshold):
        self.learning_week_period = 0
        self.period = parameters[0]
        self.m_avg_period = parameters[1]
        self.dist_period = parameters[2]
        self.dist_radius = parameters[3]
        self.data_week_avg = None
        self.streaming_start_date = None
        self.start = False
        self.level_threshold = level_threshold
        self.profile = None
        self.quantiles = defaultdict(list)
        self.distribution_period= distribution_period # in minutes
        self.distribution = distribution
        self.max_spread=0

    # Resample
    def get_time_series_rs(self, data):
        data.index = pd.to_datetime(data.timestamp, format='%Y-%m-%d %H:%M:%S')
        self.compute_max_spread(data)
        d_min = data.drop(data.columns[1:], axis=1)
        d_min = d_min.resample(self.period).count()
        return d_min

    # return the max period between over a period
    def compute_max_spread(self, data):
        self.max_spread = max(self.max_spread, np.max(data.index[1:]-data.index[:-1]))

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

    # Online Mean not necessary as we will plot distribution
    # contiguous data
    # def compute_mean(self, streaming_data):
    #     if not self.start:
    #         self.streaming_start_date = streaming_data.index[0]
    #     delta = (streaming_data.index[0] - self.streaming_start_date).week
    #     self.week_number += delta
    #     minute = streaming_data.loc[0, 'minutes']
    #     last_minute = streaming_data.loc[-1, 'minutes']
    #     hour = streaming_data.loc[0, 'hours']
    #     weekday = streaming_data.loc[0, 'weekday']
    #     self.profile_type.loc[weekday][hour][minute: last_minute].values = self.profile_type.loc[weekday][hour][
    #                                                                        minute: last_minute].values \
    #                                                                        + (streaming_data -
    #                                                                           self.profile_type.loc[weekday][hour][
    #                                                                           minute: last_minute].values) / \
    #                                                                        self.learning_week_period

    # compute distance the size of the window of the streaming batch should be
    # arrange before calling this function
    def compute_distance(self, streaming_data, profile_type):

        minute = streaming_data.loc[0, 'minutes']
        hour = streaming_data.loc[0, 'hours']
        weekday = streaming_data.loc[0, 'weekday']
        date = weekday*1440+hour*60+minute

        d = fastdtw(profile_type.loc[weekday][hour][minute:minute + self.dist_period].values,
                    streaming_data[weekday][hour][minute:minute + self.dist_period].values,
                    self.dist_radius, dist=euclidean)[0]
        return d,date

    # compute quantiles and see if d belongs to
    # improvement store the quantiles
    def threshold(self, d, ind):
        if self.quantiles[ind] is None:
            quant = mquantiles(self.distribution[ind],prob=[self.level_threshold, (1-self.level_threshold)])
        else:
            quant = self.quantiles[ind]

        if d < quant[0] or d > quant[1] :
            return False
        else:
            return True

    # adding or not the distance to the actual distribution
    # frequentist view
    def add_to_dist(self, dist_score, date):
        ind = (24*60*7) // self.distribution_period
        if self.threshold(dist_score, ind):
            self.distribution[ind].add(dist_score)
        else:
            print("Alert Anomaly detected")


