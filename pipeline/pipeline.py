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

#input_algorithm: el algoritmo que se utilizo para hacer el parsing.
#framing_technique: tecnica para seleccionar eventos contenido en un event count vector(window, sliding, events).
#input_dir: ruta a los archivos input (Ruta absoluta).
#structured_data: nombre del archivo que contiene los logs originales de manera estructurada.
#events_file: lista de los eventos identificados por el parser.
#output_dir: ruta del directorio donde se guadara el resultado de event cout matrix (Ruta absoluta).
#output_file_name: nombre del archivo que contiene el event count matrix.

    class FeatureExtractor:
      def __init__(self, input_algorithm, framing_technique, input_dir, structured_data, events_file,
                  output_dir, output_file_name):
        self.input_algorithm = input_algorithm
        self.framing_technique = framing_technique
        self.input_dir = input_dir
        self.structured_data = structured_data
        self.events_file = events_file
        self.output_dir = output_dir
        self.output_file_name = output_file_name

      def execute(*args):
        #TODO: debo incluir una condicion basado en el input_algorithm?aparentemente todos en dev generan el mismo par de archivos
        if self.framing_technique == 'fixed_window':
          self.fixed_window()

      #TODO: Funciona aislado, debe probarse
      #TODO: este objeto tiene todo las rutas que te indican de donde iniciar el proceso y en donde dejar el resultado
      def fixed_window(self):
        d = {'JAN':1, 'FEB':2, 'MAR':3, 'APR':4, 'MAY':5, 'JUN':6, 'JUL':7, 'AUG':8, 'SEP':9, 'OCT':10, 'NOV':11, 'DEC':12}

        #Lee el resultado del Log Parser.
        df = pd.read_csv(self.input_dir + self.structured_data, sep=',')
        #Transforma meses formato str a formato int.
        df['IMonth'] = df['IMonth'].str.upper().map(d)

        #Unifica Mes, Día y Tiempo.
        df['IDate'] = df[df.columns[1:3]].apply(lambda x: '-'.join(x.dropna().astype(int).astype(str)),axis=1)
        df['IDate'] = df['IDate'] + " " + df['ITime']

        #Transforma el str en un TimeDelta para poder sacar cuentas.
        df['IDate'] = pd.to_datetime(df['IDate'].astype(str), format="%m-%d %H:%M:%S")

        #Este dato debe llegar por parámetros (Quizás el indicativo de seconds/minutes/hours/days/etc también).
        Interval_Time = 60000

        #Hace un sort por timedeltas.
        df.sort_values('IDate', inplace=True)
        df = df.reset_index(drop=True)

        InitialFrame = datetime.datetime.strptime(str(df.IDate[df.index[0]]), "%Y-%m-%d %H:%M:%S").timestamp()
        Accumulator = InitialFrame + Interval_Time

        Total_Sum = ((df.IDate[df.index[-1]].value - df.IDate[df.index[0]].value))

        TimeframeCounter = 0

        for i, row in df.iterrows():
            ActualDateTimedelta = datetime.datetime.strptime(str(df.loc[i, 'IDate']), "%Y-%m-%d %H:%M:%S").timestamp()
            if(i == 0):
               df['Groups'] = TimeframeCounter
            elif(ActualDateTimedelta <= Accumulator):
               df.loc[i, 'Groups']= TimeframeCounter
            else:
               TimeframeCounter = TimeframeCounter + 1
               df.loc[i, 'Groups']= TimeframeCounter
               Accumulator = Accumulator + Interval_Time

        # Arma la matriz de Features contando los logs que hay por EventId en cada TimeFrame.
        df = df.groupby(by = 'Groups')['EventId'].value_counts().unstack().fillna(0)
        df.to_csv(self.output_dir + self.output_file_name + '.csv', index=False)


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
        self.loglizer_input_dir = parser_output_dir#TODO: aqui deberia ser el output del featur extractor
        self.log_seq = log_file
        self.log_analizer = Loglizer(log_analizer_algorithm, data_type, self.loglizer_input_dir, self.log_seq)

    def execute(self, *args):#TODO:creo que solo funciona con los casos default del parser, pq si le paso algo rompera lo que recibe el log_analizer
        self.parser.execute()
        self.log_analizer.execute(args)

    def get_new_parser(self):
        self.parser = Parser(self.parser_algorithm, self.input_dir, self.parser_output_dir,
                             self.log_file, self.parser_regex)  # TODO:que prodria pasr si tenemos varios parser a la vez?

    def get_new_loglizer(self):
        self.log_analizer = Loglizer(self.log_analizer_algorithm, self.data_type,
                                     self.loglizer_input_dir, self.log_seq)
