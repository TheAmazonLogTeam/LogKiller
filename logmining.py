import pandas as pd
import multiprocessing
from sklearn.feature_extraction.text import CountVectorizer
import time

class LogCountVectorizer(object):
	"""docstring for ClassName"""
	def __init__(self):

		vect = CountVectorizer(token_pattern='(.*)\n')
		self.vect = vect
		
	def fit_transform(self, parsed_logs):

		"""
		fit transform logs parsed (Date - Hostname - Message) into Count vectorizer
		Documents : (hostname, date)
		Words : Logs

		"""	
		start = time.time()

		## Initialisation

		df_parsed_logs = pd.DataFrame(data=parsed_logs, columns=[
                              'Date', 'Hostname', 'Message'])

		df_grouped_by_hostname = df_parsed_logs.groupby('Hostname')

		## Gathering all documents

		documents = []
		messages = []

		for name_h, group_h in df_grouped_by_hostname:
		    df_grouped_by_hostname_date = group_h.groupby('Date')
		    
		    for name_d, group_d in df_grouped_by_hostname_date:
		        messages.append('\n'.join(group_d['Message'].values))
		        documents.append((name_h,name_d))

		## sklearn fit_transform

		count = self.vect.fit_transform(messages)
		features = self.vect.get_feature_names()

		## multi index dataframe

		index = pd.MultiIndex.from_tuples(documents, names=['Hostname', 'Date'])
		df_count = pd.DataFrame(count.A, columns=features, index=index)

		end = time.time()
		print('[Success] Count Vectorizer done within %.2fs' % (end-start))

		return df_count


class LfdfhfTransformer(object):

	def __init__(self):
		pass

	## parallelizing functions

	def mean_column_df(self,df):
	    return df.apply(lambda x: x.mean(), axis = 0)

	def prop_row_df(self,df):
	    return df.apply(lambda x: x / x.sum() , axis = 1)

	@staticmethod
	def apply_parallel(df_grouped, func, level_name):
	    pool = multiprocessing.Pool()
	    
	    groups = []
	    names = []
	    for name, group in df_grouped:
	        groups.append(group)
	        names.append(name)
	        
	    ret_list = pool.map(func, groups)
	    pool.close()
	    pool.join()
	    
	    index = pd.Index(names, name=level_name)
	    return pd.DataFrame(ret_list, index=index)



	## transformer functions

	def lf_transformer(self, df_count):
		"""
		Log frequency inside one document
		"""
		df_log_frequency = df_count.apply(lambda x: x / x.sum(), axis = 1) # à paralelliser
		return df_count

	def df_transformer(self, df_count):
		"""
		Log frequency outside one date document
		"""
		df_per_date_frequency = self.apply_parallel(df_count.astype(
    bool).astype(int).groupby(axis=0, level=1), self.mean_column_df, level_name="Date")

	def hf_transformer(self, df_count):
		"""
		Log frequency outside one hostname document
		"""
		df_per_date_frequency = self.apply_parallel(df_count.astype(
    bool).astype(int).groupby(axis=0, level=0), self.mean_column_df, level_name="Hostname")

	def lfdfhf_transformer(self, df_count):
		start = time.time()
		df_lf = self.lf_transformer(df_count)
		df_df = self.df_transformer(df_count)
		df_hf = self.hf_transformer(df_count)
		end = time.time()
		print('[Success] Lfdfhf Transformation done within %.2fs *paralellized*' % (end-start))

		return df_lf, df_df, df_hf


