import sys
import os
import time

from pipeline import Pipeline
os.chdir('/loglizer')
sys.path.append('/loglizer')#drain __init__.py
PYTHONPATH= '/loglizer'
# ro1 = open("/rocore1.log")
# ro2 = open("/rocore2.log")
#
# print(ro1.read())
# print('******')
# print(ro2.read())

# os.rename("/rocore2.log", "/dayco_log.log")


# file = open('/dayco_log.log', 'r')
# lines = file.readlines()
# file.close()
# file = open('/dayco_log.log', 'a')
# file.write(lines[-1][:-1])
# file.close()
# file = open('/dayco_log.log', 'r')
# lines = file.readlines()
# file.close()

# print(len(lines))
# print(lines[-2:])

input_dir  = '/'  # The input directory of log file
output_dir = '/'  # The output directory of parsing results
log_file   = 'dayco_log.log'  # The input log file name
#log_format = '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>'#BGL
log_format = '<smonth> <sday> <shour> <ip> <id> <id2> <month> <day> <hour> <city> <type> <Content>'#dayco/rsyslog

pipeline = Pipeline(parser_algorithm='drain', input_dir = input_dir, parser_output_dir = output_dir, log_file = log_file,
parser_regex = log_format, feature_extractor='fixed_window', log_analizer_algorithm='mining_invariants',
data_type='time_based')

para = {
'path':'/',                 # directory for input data
'log_file_name':'dayco_log.log',           # filename for log data file
'log_event_mapping':'dayco_log.logTemplateMap.csv',   # filename for log-event mapping. A list of event index, where each row represents a log
'save_path': '../time_windows/',             # dir for saving sliding window data files to avoid splitting
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
#event_count_matrix = pipeline.log_analizer.get_event_count_matrix(para)
#invar_dict = pipeline.log_analizer.find_invariants(para, event_count_matrix)
#predictions, anomalies = pipeline.log_analizer.get_anomalies(para, event_count_matrix, invar_dict)
pipeline.initial_go(para)
end = time.time()
print('total execution time in seconds?',end - start)

# head /rocore1.log
# print('******')
# head /rocore2.log
