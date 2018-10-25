#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'Elieser Pereira'

#parser algoritmhs import
from logparser.logparser.Drain import Drain
from logparser.logparser.LogSig import LogSig
from logparser.logparser.LenMa import LenMa
#analizer algorithm imports
from utils import data_loader as data_loader
from models import mining_invariants as mi


class Parser:
    def __init__(self, algorithm):
        self.algorithm = algorithm

    def execute(self, *args):  # ver como es que se define el excute por default para ejecutar directamente de instancia de la clase
        if self.algorithm == 'drain':
            self.drain()
        elif self.algorithm == 'logsig':
            self.logsig()
        elif self.algorithm == 'lenma':
            self.lenma()

    # input_dir: The input directory of log file
    # output_dir: The output directory of parsing results)
    # log_file: The input log file name
    # log_format: HDFS log format
    # regex: Regular expression list for optional preprocessing (default: [])
    def drain(self, input_dir='/loglizer/logparser/logs/HDFS/',
              output_dir='Drain_result/',
              log_file='HDFS_2k.log',
              log_format='<Date> <Time> <Pid> <Level> <Component>: <Content>',
              regex=[
                  r'blk_(|-)[0-9]+',  # block id
                  r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)',  # IP
                  r'(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$',  # Numbers
              ],
              st=0.5,  # Similarity threshold
              depth=4  # Depth of all leaf nodes
              ):

        parser = Drain.LogParser(log_format, indir=input_dir, outdir=output_dir, depth=depth, st=st, rex=regex)
        parser.parse(log_file)

    def logsig(self,
               input_dir='/loglizer/logparser/logs/HDFS/',  # The input directory of log file
               output_dir='LogSig_result/',  # The output directory of parsing results
               log_file='HDFS_2k.log',  # The input log file name
               log_format='<Date> <Time> <Pid> <Level> <Component>: <Content>',  # HDFS log format
               regex=[],  # Regular expression list for optional preprocessing (default: [])
               group_number=14,  # The number of message groups to partition
               ):
        parser = LogSig.LogParser(input_dir, output_dir, group_number, log_format, rex=regex)
        parser.parse(log_file)

    def lenma(self,
              input_dir='/loglizer/logparser/logs/HDFS/',  # The input directory of log file
              output_dir='/loglizer/Lenma_result/',  # The output directory of parsing results
              log_file='HDFS_2k.log',  # The input log file name
              log_format='<Date> <Time> <Pid> <Level> <Component>: <Content>',  # HDFS log format
              threshold=0.9,  # TODO description (default: 0.9)
              regex=[],  # Regular expression list for optional preprocessing (default: [])
              ):

        parser = LenMa.LogParser(input_dir, output_dir, log_format, threshold=threshold, rex=regex)
        parser.parse(log_file)


# algorithm: algoritmo a utilizar
# data_type: time_dased, event_based
# NOTA: pareciera que si es timebased el minign invariants incluye un sliding window
class Loglizer:
    def __init__(self, algorithm, data_type):
        self.algorithm = algorithm
        self.data_type = data_type

    def execute(self, *args):
        if self.algorithm == 'mining_invariants':
            self.mining_invariants(*args)

    def mining_invariants(self, para):
        print(para)
        if self.data_type == 'time_based':
            raw_data, event_mapping_data = data_loader.bgl_data_loader(para)
            event_count_matrix, labels = data_loader.bgl_preprocess_data(para, raw_data, event_mapping_data)
            r = mi.estimate_invar_spce(para, event_count_matrix)
            invar_dict = mi.invariant_search(para, event_count_matrix, r)
            mi.evaluate(event_count_matrix, invar_dict, labels)
        elif self.data_type == 'event_based':
            raw_data, label_data = data_loader.hdfs_data_loader(para)
            r = mi.estimate_invar_spce(para, raw_data)
            invar_dict = mi.invariant_search(para, raw_data, r)
            mi.evaluate(raw_data, invar_dict, label_data)


class Pipeline:
    def __init__(self, parser='drain', feature_extractor='fixed_window', log_analizer='mining_invariants'):
        self.parser = Parser(parser)  # TODO:que prodria pasr si tenemos varios parser a la vez?
        # self.feature_extractor = FeatureExtractor(feature_extractor)
        self.log_analizer = Loglizer(log_analizer)

    def execute(self, *args):
        self.parser.execute()
        self.log_analizer.execute()
