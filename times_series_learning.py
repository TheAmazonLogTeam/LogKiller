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


class TimesSeriesLearning(object):

    def __init__(self, parameters, distribution_period, distribution, level_threshold, lwp=0):
        self.learning_week_period = lwp # seconds between first and last element
        self.period = parameters[0]  # Sampling Period
        self.m_avg_period = parameters[1]  # m_avg_period
        self.dist_period = parameters[2]   # distance period evaluation
        self.dist_radius = parameters[3]   # distance window
        self.data_week_avg = None
        self.streaming_start_date = None
        self.start = False
        self.level_threshold = level_threshold
        self.profile = None
        self.quantiles = defaultdict(list)
        self.distribution_period = distribution_period  # in minutes
        self.distribution = distribution
        self.max_spread = 0
        self.min_spread = np.inf


    # Resample
    def get_time_series_rs(self, data):
        #t0 = time.time()
        data.index = pd.to_datetime(data.timestamp, format='%Y-%m-%d %H:%M:%S')
        self.compute_max_spread(data)
        d_min = data.drop(data.columns[1:], axis=1)
        d_min = d_min.resample(str(self.period)+'min').count()
        #print('Resample processed in:', time.time()-t0)
        return d_min

    # return the max period between over a period
    def compute_max_spread(self, data):
        #t0 = time.time()
        if data.shape[0] > 2:
            d = (data.index[1:] - data.index[:-1])
            self.max_spread = max(self.max_spread, np.amax(d.seconds.values))
            self.min_spread = min(self.min_spread, np.amin(d.microseconds.values))
        elif data.shape[0] == 2:
            d = (data.index[0] - data.index[1])
            self.max_spread = max(self.max_spread, np.amax(d.seconds))
            self.min_spread = min(self.min_spread, np.amin(d.microseconds))
        else:
            self.max_spread = self.learning_week_period
            self.min_spread = self.learning_week_period

        #print('Max spread processed in:', time.time() - t0)

    # Moving average
    def mov_avg(self, data):
        return data.rolling(window=int(self.m_avg_period)).mean().fillna(method='backfill')

    # Dataframe for grouping per week
    def weekly_average(self, data):
        data['minutes'] = data.index.minute
        data['hour'] = data.index.hour
        data["weekday"] = data.index.weekday
        data["weekday_name"] = data.index.weekday_name
        return data.groupby(['weekday_name', 'hour', 'minutes']).mean()['timestamp']

    # Set the profile

    def set_profile(self, data, gen):
        data_rs = self.get_time_series_rs(data)

        if gen:
            self.learning_week_period = (data_rs.index[0] - data_rs.index[-1]).seconds

        self.profile = self.weekly_average(self.mov_avg(data_rs))

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
        date = weekday * 1440 + hour * 60 + minute

        d = fastdtw(profile_type.loc[weekday][hour][minute:minute + self.dist_period].values,
                    streaming_data[weekday][hour][minute:minute + self.dist_period].values,
                    self.dist_radius, dist=euclidean)[0]
        return d, date

    # compute quantiles and see if d belongs to
    # improvement store the quantiles
    def threshold(self, d, ind):
        if self.quantiles[ind] is None:
            quant = mquantiles(self.distribution[ind], prob=[self.level_threshold, (1 - self.level_threshold)])
        else:
            quant = self.quantiles[ind]

        if d < quant[0] or d > quant[1]:
            return False
        else:
            return True

    # adding or not the distance to the actual distribution
    # frequentist view
    def add_to_dist(self, dist_score, date):
        ind = (24 * 60 * 7) // self.distribution_period
        if self.threshold(dist_score, ind):
            self.distribution[ind].add(dist_score)
        else:
            print("Alert Anomaly detected")
