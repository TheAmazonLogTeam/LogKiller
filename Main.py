import pandas as pd
from Reader import Reader
from Parser import Parser
from logmining import LogCountVectorizer, LfdfhfTransformer

if __name__ == '__main__':

	reader = Reader()
	parser = Parser()

	# Gathering all logs in one list
	logs = reader.read_dir('../logs/*',10)

	# Parse logs into Date-Hostname-Message
	parsed_logs = parser.parse_all(logs)

	# Log Count Vectorizer
	lcv = LogCountVectorizer()
	df_count = lcv.fit_transform(parsed_logs)

	# lfdfhf Transformer
	lfdfhf = LfdfhfTransformer()
	df_lf, df_df, df_hf = lfdfhf.lfdfhf_transformer(df_count)

	print(df_df[df_df['Log'] == "bonjour"])