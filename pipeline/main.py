import time
import sys

repo_path = '/home/us1/git/loglizer'
sys.path.append(repo_path)#logsig __init__.py
sys.path.append(repo_path + '/logparser/logparser/LogSig')#logsig __init__.py
sys.path.append(repo_path + '/logparser/logparser/LenMa/')#for lenma __init__.py
sys.path.append(repo_path + '/logparser/logparser/LenMa/templateminer')#for lenma
from pipeline.pipeline import Pipeline

input_dir  = repo_path +'/'  # The input directory of log file
output_dir = repo_path + '/'  # The output directory of parsing results
log_file   = 'dayco_log.log'  # The input log file name
log_format = '<smonth> <sday> <shour> <ip> <id> <id2> <month> <day> <hour> <city> <type> <Content>'#dayco/rsyslog

pipeline = Pipeline(parser_algorithm='drain', input_dir = input_dir, parser_output_dir = output_dir, log_file = log_file,
parser_regex = log_format, feature_extractor='fixed_window', log_analizer_algorithm='mining_invariants',
data_type='time_based', elasticsearch_index_name='deepia')

para = {
'path':repo_path + '/',                 # directory for input data
'log_file_name':'dayco_log.log',           # filename for log data file
'log_event_mapping':'dayco_log.logTemplateMap.csv',   # filename for log-event mapping. A list of event index, where each row represents a log
'save_path': './time_windows/',             # dir for saving sliding window data files to avoid splitting
#'select_column':[0,4],                      # select the corresponding columns (label and time) in the raw log file
'select_column':[0,1,2],                      # select the corresponding columns (label and time) in the raw log file, en caso de depia no nos intersa la columna label
'window_size':3, #3                           # time window (unit: hour)
'step_size': 1,                             # step size (unit: hour)
'epsilon':2.0,                              # threshold for the step of estimating invariant space
'threshold':0.97,                           # percentage of vector Xj in matrix satisfies the condition that |Xj*Vi|<epsilon
'scale_list':[1,2,3],					    # list used to sacle the theta of float into integer
'stop_invar_num':3                          # stop if the invariant length is larger than stop_invar_num. None if not given
}
start = time.time()
pipeline.initial_go(para)
end = time.time()
print('\n total execution time in seconds?',end - start)

pipeline.run_online(para, 600, 30, 10)