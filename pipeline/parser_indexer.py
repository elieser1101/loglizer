#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'Elieser Pereira'
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

class ParsedFile:
    def __init__(self, structure_logfile_path):
        self.parsed_logs_list = list()
        self.structure_logfile_path = structure_logfile_path
        self.structured_log_df = None
        self.load()

    def load(self):
        self.structured_log_df = pd.read_csv(self.structure_logfile_path)
        for index, row in self.structured_log_df.iterrows():
            log = ParsedLog(row.LineId,
                            row.smonth,
                            row.sday,
                            row.shour,
                            row.ip,
                            row.id,
                            row.id2,
                            row.month,
                            row.day,
                            row.hour,
                            row.city,
                            row.type,
                            row.Content,
                            row.EventId,
                            row.EventTemplate
                            )
            self.parsed_logs_list.append(log)
            # TODO: este parsed log dependera del tipoi de log, esta clase es hardcode dayco, pero deberia ser capaz de funcionar con cualquier log


class ParsedLog:
    def __init__(self, line_id, smonth, sday, shour, ip, id1, id2, month, day,
                 hour, city, log_type, content, event_id, event_template):
        self.line_id = line_id
        self.smonth = smonth
        self.sday = sday
        self.shour = shour
        self.ip = ip
        self.id1 = id1
        self.id2 = id2
        self.month = month
        self.day = day
        self.hour = hour
        self.city = city
        self.log_type = log_type
        self.content = content
        self.event_id = event_id
        self.event_template = event_template
        self.timestamp = self.month + '-' + str(self.day) + '-' + self.hour
        self.syslog_timestamp = self.smonth + '-' + str(self.sday) + '-' + self.shour
        # matching attrs
        self.anomaly = False
        self.anomaly_vector_id = -1
        # TODO:agregar los capos para asociar con las anomallias, is_anomaly??habia otro??o solo con el objeto anomaly tambien puedo seleccinar las lineas

    def to_dict(self):
        return self.__dict__


class Indexer:
    def __init__(self, index_name):
        self.es = Elasticsearch()
        self.index_name = index_name

    def index_log(self, log):
        response = self.es.index(index=self.index_name,
                                 doc_type='log',
                                 body=log,
                                 id=log['line_id'])

    def update_log(self, log, doc_id):
        response = self.es.update(index=self.index_name,
                                  doc_type='log',
                                  body=log,
                                  id=doc_id)

    # TODO:que pasa cuando indexo un nuevo file???
    # TODO:cuando ejecuto esto????
    def index_file(self, parsed_file):
        for parsed_log in parsed_file.parsed_logs_list:
            parsed_log_dict = parsed_log.to_dict()
            self.index_log(parsed_log_dict)

    def get_index(self):
        s = Search(using=self.es, index=self.index_name)
        res = s.scan()
        return res

    def delete_index(self, index_name):
        self.es.indices.delete(index=index_name)
