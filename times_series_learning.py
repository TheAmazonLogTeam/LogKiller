import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import datetime as dt
from scipy.spatial.distance import euclidean
from fastdtw import fastdtw
from scipy.stats.mstats import mquantiles
import calendar
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

    def __init__(self, parameters, distribution_period, level_threshold, timestamp_anomaly, processus=True):
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
        self.learning_week_period = None
        self.processus = processus
        self.timestamp_anomaly = timestamp_anomaly
        # print('Initialisation Done')
        # print('Sampling Period: ', self.period)
        # print('Moving Average Period ( DTW only): ', self.m_avg_period)
        # print('Metric Period evaluation in minutes: ', self.dist_period)
        # print('DTW radius in minutes: ', self.dist_radius)

    # Resample
    def get_time_series_rs(self, data, streaming=False):
        if streaming:
            if not self.processus:
                d_min = data.resample(str(self.period) + 'min').count()
                d_min = pd.DataFrame(d_min,
                                     columns=['timestamp'],
                                     index=pd.date_range(start=d_min.index[0],
                                                         periods=int(self.dist_period), freq='min')).fillna(0)
            else:

                data = data.drop(data.columns[1:len(data.columns) - 1], axis=1) # Drop cluster columns
                d_min = data.resample(str(self.period) + 'S').count().cumsum()  # resample to 1 sec
                d_min = pd.DataFrame(d_min,                                     # resample to the compute metric period
                                     columns=['timestamp'],
                                     index=pd.date_range(start=d_min.index[0],
                                                         periods=int(self.dist_period) * 60,
                                                         freq='S')).fillna(d_min.timestamp.values[-1])


        else:
            if self.processus:
                d_min = data.resample(str(self.period) + 'S').count().cumsum()
                d_min = pd.DataFrame(d_min, columns=['timestamp'],
                                     index=pd.date_range(start=d_min.index[0],
                                                         periods=24*3600*6,
                                                         freq='S')).fillna(d_min.timestamp.values[-1])

            else:
                d_min = data.resample(str(self.period) + 'min').count()

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
        data['second'] = data.index.second
        data['minute'] = data.index.minute
        data['hour'] = data.index.hour
        data["weekday"] = data.index.weekday
        return data.groupby(['weekday', 'hour', 'minute']).mean()['timestamp']

    # Set the profile

    def set_profile(self, data):
        data.index = pd.to_datetime(data.timestamp, format='%Y-%m-%d %H:%M:%S')
        data_rs= data.drop(data.columns[1:len(data.columns) - 1], axis=1)
        data_rs = self.get_time_series_rs(data_rs)
        self.learning_week_period = data_rs.shape[0]
        self.compute_max_spread(data)
        if self.processus:
            self.profile = data_rs
        else:
            self.profile = self.weekly_average(self.mov_avg(data_rs))

    def compute_distance_profile(self, data, distribution, measures, train_mode, verbose=False):
        anomaly = False
        threshold = True
        data.index = pd.to_datetime(data.timestamp, format='%Y-%m-%d %H:%M:%S')
        anomaly, max_spread, min_spread = self.eval_max_spread(data)
        if self.processus:
            data_rs = self.get_time_series_rs(data, True)
            d, date = self.compute_integral(data_rs, self.profile)
        else:
            data_rs = self.weekly_average(self.mov_avg(self.get_time_series_rs(data, True)))
            d, date = self.compute_distance(data_rs, self.profile)
        m = self.compute_spread_metric(data_rs)
        threshold, quant = self.add_to_dist(d, m,  date, distribution, measures, train_mode)

        # if anomaly or not threshold:
        #     print("Anomaly detected \n")
        #     print("log spread anomaly", anomaly)
        #     print("profile_distance anomaly", not threshold)
        #     print("distance detected is:", d)
        # elif verbose:
        #     print("Batch correct \n")
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

    def compute_integral(self, streaming_data, profile_type):
        sec = streaming_data.index[0].second
        minute = streaming_data.index[0].minute
        hour = streaming_data.index[0].hour
        weekday = streaming_data.index[0].weekday()
        mask = (profile_type.index.weekday == weekday) & \
               (profile_type.index.hour == hour) & \
               (profile_type.index.minute == minute) & \
               (profile_type.index.second == sec)
        start_date = profile_type.loc[:, 'timestamp'].index[0]
        if len(profile_type.loc[mask,'timestamp'].index) > 0:
            start_date = profile_type.loc[mask, 'timestamp'].index[0]

        if start_date + dt.timedelta(seconds=self.dist_period * 60 - 1) <= profile_type.index[-1]:
            if start_date > profile_type.index[0]:
                ref = np.subtract(profile_type.loc[start_date: start_date + dt.timedelta(seconds=self.dist_period*60-1),'timestamp'].values,
                              profile_type.loc[start_date-dt.timedelta(seconds=1), 'timestamp'])
            else:
                ref = profile_type.loc[start_date: start_date + dt.timedelta(seconds=self.dist_period*60-1),'timestamp'].values

            d = np.sum(np.subtract(ref, streaming_data.values.ravel()))
        else:
            d=0
        return d, streaming_data.index[0]

    def compute_spread_metric(self, data):
        metric = np.max(data[1:]-data[:-1])
        return metric

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
    def add_to_dist(self, dist_score, metric,  date, distribution, measure, train_mode):
        anomaly = dict()
        level_ok = True
        quant = 0.1
        minute = date.minute
        hour = date.hour
        weekday = date.weekday()
        d = weekday*24*3600 + hour*3600 + minute*60

        if self.processus:
            ind = d // (self.distribution_period*60)
        else:
            ind = d // self.distribution_period
        measure[ind]['Area_Difference'].append(dist_score)
        measure[ind]['Max_Spread'].append(metric)

        if not train_mode:
            level_ok, quant = self.threshold(dist_score, ind, distribution)
            if level_ok:
                distribution[int(ind)].add(float(dist_score))
            else:
                #print("Alert Anomaly detected, the distance is in the " + str(self.level_threshold))
                #print("Timestamp is:", str(date))
                #print("Area difference value is : ", dist_score)
                anomaly['Area_Difference'] = dist_score
                anomaly['Timestamp'] = date
                self.timestamp_anomaly = pd.concat([self.timestamp_anomaly,pd.DataFrame([anomaly])],
                                                   axis=0)

        else:
            distribution[int(ind)].add(float(dist_score))
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
