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

    def __init__(self, parameters, distribution_period, level_threshold, processus):
        # self.learning_week_period = lwp  # seconds between first and last element
        self.period = parameters[0]  # Sampling Period
        self.m_avg_period = parameters[1]  # m_avg_period
        self.dist_period = parameters[2]  # distance period evaluation
        self.dist_radius = parameters[3]  # distance window
        self.data_week_avg = None
        self.streaming_start_date = None
        self.start = False
        self.level_threshold = level_threshold
        self.profile = None
        self.distribution_period = distribution_period  # in minutes
        self.max_spread = 0
        self.min_spread = np.inf
        self.processus = processus

    # Resample
    def get_time_series_rs(self, data, streaming=False):
        # t0 = time.time()
        d_min = data.drop(data.columns[1:], axis=1)
        if self.processus:
            d_min = d_min.resample(str(self.period) + 'min').cumsum()
        else:
            d_min = d_min.resample(str(self.period) + 'min').count()
        if streaming:
            data_count = np.zeros(15, dtype=int)
            data_count[:d_min.timestamp.values.shape[0]] = d_min.timestamp.values
            d_min = pd.DataFrame(data_count,
                                 columns=['timestamp'],
                                 index=pd.date_range(start=data.index[0],
                                                     periods=int(self.dist_period), freq='min')).fillna(0)
        # print('Resample processed in:', time.time()-t0)
        return d_min

    # return the max period between over a period
    def compute_max_spread(self, data):

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

    def eval_max_spread(self, data):
        anomaly_present = False
        if data.shape[0] > 2:
            d = (data.index[1:] - data.index[:-1])
            max_spread = np.amax(d.seconds.values)
            min_spread = np.amin(d.microseconds.values)
        elif data.shape[0] == 2:
            d = (data.index[0] - data.index[1])
            max_spread = d.seconds
            min_spread = d.microseconds
        else:
            max_spread = self.max_spread
            min_spread = self.min_spread
        if max_spread > self.max_spread:
            print("Anomaly Detected max_spread higher than usual ")
            anomaly_present = True
        if min_spread < self.min_spread:
            print("Anomaly Detected min_spread lower than usual ")
            anomaly_present = True
        return anomaly_present, min_spread, max_spread

    # Moving average
    def mov_avg(self, data):
        return data.ewm(com=int(self.m_avg_period), adjust=True).mean().fillna(method='backfill')

    # Dataframe for grouping per week
    def weekly_average(self, data):
        data['minute'] = data.index.minute
        data['hour'] = data.index.hour
        data["weekday"] = data.index.weekday
        data["weekday_name"] = data.index.weekday_name
        return data.groupby(['weekday', 'hour', 'minute']).mean()['timestamp']

    # Set the profile

    def set_profile(self, data):
        data.index = pd.to_datetime(data.timestamp, format='%Y-%m-%d %H:%M:%S')
        data_rs = self.get_time_series_rs(data)
        self.learning_week_period = (data_rs.index[0] - data_rs.index[-1]).seconds
        self.compute_max_spread(data)
        if self.processus:
            self.profile = self.weekly_average(data_rs)
        else:
            self.profile = self.weekly_average(self.mov_avg(data_rs))

    def compute_distance_profile(self, data, distribution, verbose=False):
        anomaly = False
        threshold = False
        data.index = pd.to_datetime(data.timestamp, format='%Y-%m-%d %H:%M:%S')
        anomaly, max_spread, min_spread = self.eval_max_spread(data)
        if self.processus :
            data_rs = self.weekly_average(self.get_time_series_rs(data, True))
            d, date = self.compute_integral(data_rs, self.profile)
        else:
            data_rs = self.weekly_average(self.mov_avg(self.get_time_series_rs(data, True)))
            d, date = self.compute_distance(data_rs, self.profile)
        threshold, quant = self.add_to_dist(d, date, distribution)

        if anomaly or not threshold:
            print("Anomaly detected \n")
            print("log spread anomaly", anomaly)
            print("profile_distance anomaly", not threshold)
            print("distance detected is:", d)
        elif verbose:
            print("Batch correct \n")
        return anomaly, max_spread, min_spread, d, date, threshold, quant

    def compute_distance(self, streaming_data, profile_type):

        minute = streaming_data.index.get_level_values('minute')[0]
        hour = streaming_data.index.get_level_values('hour')[0]
        weekday = streaming_data.index.get_level_values('weekday')[0]
        date = weekday * 1440 + hour * 60 + minute
        # print('caca', streaming_data[weekday][hour][minute:int(minute + self.dist_period)].values)
        # print('pipi',profile_type[weekday][hour][minute:int(minute + self.dist_period)].values)
        d, _ = fastdtw(profile_type[weekday][hour][minute:int(minute + self.dist_period)].values,
                       streaming_data[weekday][hour].values,
                       radius=int(self.dist_radius), dist=euclidean)
        # print('distance: ', d)
        return d, date

    def compute_integral(self,streaming_data, profile_type):
        minute = streaming_data.index.get_level_values('minute')[0]
        hour = streaming_data.index.get_level_values('hour')[0]
        weekday = streaming_data.index.get_level_values('weekday')[0]
        date = weekday * 1440 + hour * 60 + minute
        # print('caca', streaming_data[weekday][hour][minute:int(minute + self.dist_period)].values)
        # print('pipi',profile_type[weekday][hour][minute:int(minute + self.dist_period)].values)
        d, _ = np.sum(np.subtract(profile_type[weekday][hour][minute:int(minute + self.dist_period)].values,
                                  streaming_data[weekday][hour].values)*self.period)
        # print('distance: ', d)
        return d, date




    # compute quantiles and see if d belongs to
    def threshold(self, d, ind, distribution):
        # print(distribution)
        quant = mquantiles(distribution[int(ind)], prob=[self.level_threshold, (1 - self.level_threshold)])
        if len(quant) < 2:
            return True, quant
        if d > quant[1]:
            return False, quant
        else:
            return True, quant

    # adding or not the distance to the actual distribution
    # frequentist view
    def add_to_dist(self, dist_score, date, distribution):

        ind = date // self.distribution_period
        level_ok, quant = self.threshold(dist_score, ind, distribution)
        if level_ok:
            distribution[int(ind)].add(float(dist_score))
        else:
            print("Alert Anomaly detected, the distance is in the " + str(self.level_threshold))
        return level_ok, quant

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
