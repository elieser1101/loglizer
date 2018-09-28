#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'Shilin He'

from models import mining_invariants as mi

def hdfs_data_loader(para):
	""" load the log sequence matrix and labels from the file path.

	Args:
	--------
	para: the parameters dictionary

	Returns:
	--------
	raw_data:  log sequences matrix
	label_data: labels matrix
	"""
	file_path = para['path'] + para['log_seq_file_name']
	label_path = para['path'] + para['label_file_name']
	# load log sequence matrix
	pre_df = pd.read_csv(file_path, nrows=1, header=None, delimiter=r'\s+')
	columns = pre_df.columns.tolist()
	# remove the last column of block name
	use_cols = columns[:-1]
	data_df = pd.read_csv(file_path, delimiter=r'\s+', header=None, usecols =use_cols, dtype =int)
	raw_data = data_df.as_matrix()
	# load lables
	label_df = pd.read_csv(label_path, delimiter=r'\s+', header=None, usecols = [0], dtype =int) # usecols must be a list
	label_data = label_df.as_matrix()
	print("The raw data shape is {} and label shape is {}".format(raw_data.shape, label_data.shape))
	assert raw_data.shape[0] == label_data.shape[0]
	print('The number of anomaly instances is %d' % sum(label_data))
	return raw_data, label_data

para = {
'path':'/logalizardata/Data/SOSP_data/',        # directory for input data
'log_seq_file_name':'rm_repeat_rawTFVector.txt', # filename for log sequence data file
'label_file_name':'rm_repeat_mlabel.txt', # filename for label data file
'epsilon':2.0,                          # threshold for the step of estimating invariant space
'threshold':0.98,                       # percentage of vector Xj in matrix satisfies the condition that |Xj*Vi|<epsilon
'scale_list':[1,2,3],					# list used to sacle the theta of float into integer
'stop_invar_num':3                      # stop if the invariant length is larger than stop_invar_num. None if not given
}

if __name__ == '__main__':
	raw_data, label_data = data_loader.hdfs_data_loader(para)
	r = mi.estimate_invar_spce(para, raw_data)
	invar_dict = mi.invariant_search(para, raw_data, r)
	mi.evaluate(raw_data, invar_dict, label_data)
