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


#TODO: es necesario conocer el log format siempree???como mejoramos esto??
class Parser:
    #algorithm = algoritmo del parser
    #input_dir = directorio donde esta el log a parsear
    #ouput_dir = donde se guardara el resultado del parsing
    #log_file = nombre del log
    #regex = regex que utilizara el parser, #TODO:deberia ser parametro de la clase??o solo del metodo que se implemente??
    def __init__(self, algorithm, input_dir, output_dir, log_file, log_format, regex=None):
        self.algorithm = algorithm
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.log_file = log_file
        self.log_format = log_format
        self.regex = regex
    def execute(self, *args):  # ver como es que se define el excute por default para ejecutar directamente de instancia de la clase
        if self.algorithm == 'drain':
            self.drain(args)
        elif self.algorithm == 'logsig':
            self.logsig(args)
        elif self.algorithm == 'lenma':
            self.lenma(args)

    # input_dir: The input directory of log file
    # output_dir: The output directory of parsing results)
    # log_file: The input log file name
    # log_format: log format
    # regex: Regular expression list for optional preprocessing (default: [])
    #TODO:que hacemos con la regex??
    def drain(self,
              regex=[
                  r'blk_(|-)[0-9]+',  # block id
                  r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)',  # IP
                  r'(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$',  # Numbers
              ],
              st=0.5,  # Similarity threshold
              depth=4  # Depth of all leaf nodes
              ):

        parser = Drain.LogParser(self.log_format, indir=self.input_dir, outdir=self.output_dir, depth=depth, st=st, rex=regex)
        parser.parse(self.log_file)

    # input_dir: The input directory of log file
    # output_dir: The output directory of parsing results)
    # log_file: The input log file name
    # log_format: log format
    # regex: Regular expression list for optional preprocessing (default: [])
    def logsig(self,
               regex=[],  # Regular expression list for optional preprocessing (default: [])
               group_number=14,  # The number of message groups to partition
               ):
        parser = LogSig.LogParser(self.input_dir, self.output_dir, group_number, self.log_format, rex=regex)
        parser.parse(self.log_file)

    # input_dir: The input directory of log file
    # output_dir: The output directory of parsing results)
    # log_file: The input log file name
    # log_format: log format
    # regex: Regular expression list for optional preprocessing (default: [])
    def lenma(self,
              threshold=0.9,  # TODO description (default: 0.9)
              regex=[],  # Regular expression list for optional preprocessing (default: [])
              ):

        parser = LenMa.LogParser(self.input_dir, self.output_dir, self.log_format, threshold=threshold, rex=regex)
        parser.parse(self.log_file)


# algorithm: algoritmo a utilizar
# data_type: time_dased, event_based
# NOTA: pareciera que si es timebased el minign invariants incluye un sliding window
class Loglizer:
    def __init__(self, algorithm, data_type, input_dir, log_seq):
        self.algorithm = algorithm
        self.data_type = data_type
        self.input_dir = input_dir
        self.log_seq = log_seq

    def execute(self, *args):
        if self.algorithm == 'mining_invariants':
            self.mining_invariants(*args)

    #TODO: como manejo que este metodo recibe un dict??????
    def mining_invariants(self, para):
        para['path'] = self.input_dir
        print(para)
        if self.data_type == 'time_based':
            para['log_file_name'] = self.log_seq
            raw_data, event_mapping_data = data_loader.bgl_data_loader(para)
            event_count_matrix, labels = data_loader.bgl_preprocess_data(para, raw_data, event_mapping_data)
            r = mi.estimate_invar_spce(para, event_count_matrix)
            invar_dict = mi.invariant_search(para, event_count_matrix, r)
            mi.evaluate(event_count_matrix, invar_dict, labels)
        elif self.data_type == 'event_based':
            para['log_seq_file_name'] = self.log_seq
            raw_data, label_data = data_loader.hdfs_data_loader(para)
            r = mi.estimate_invar_spce(para, raw_data)
            invar_dict = mi.invariant_search(para, raw_data, r)
            mi.evaluate(raw_data, invar_dict, label_data)


class Pipeline:
    def __init__(self, parser_algorithm='drain', input_dir = None, parser_output_dir = None, log_file = None,
                 parser_regex = None, feature_extractor='fixed_window', log_analizer_algorithm='mining_invariants',
                 data_type='event_based', ):
        #TODO:es necesario cargarle estos atributos a pipeline o simplemente se los paso a la calse que instancia??
        self.parser_algorithm = parser_algorithm
        self.input_dir = input_dir
        self.parser_output_dir = parser_output_dir
        self.log_file = log_file
        self.parser_regex = parser_regex
        self.parser = Parser(parser_algorithm, input_dir, parser_output_dir, log_file, parser_regex)  # TODO:que prodria pasr si tenemos varios parser a la vez?

        # self.feature_extractor = FeatureExtractor(feature_extractor)

        #TODO:es necesario cargarle estos atributos a pipeline o simplemente se los paso a la calse que instancia??
        self.log_analizer_algorithm = log_analizer_algorithm
        self.data_type = data_type
        self.input_dir = parser_output_dir#TODO: aqui deberia ser el output del featur extractor
        self.log_seq = log_file
        self.log_analizer = Loglizer(log_analizer_algorithm, data_type, input_dir, self.log_seq)

    def execute(self, *args):#TODO:creo que solo funciona con los casos default del parser, pq si le paso algo rompera lo que recibe el log_analizer
        self.parser.execute()
        self.log_analizer.execute(args)
