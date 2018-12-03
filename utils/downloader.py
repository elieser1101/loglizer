#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'Ricardo Capuz'
# Import Elasticsearch package
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

def create_file_from_elastic(index, source_log, result_log_path):
    client = Elasticsearch()
    s = Search(using=client, index=index).query("match", source=source_log)
    res = s.scan()
    file = open(result_log_path, 'w')
    for indexed_log_line in res:
        file.write(indexed_log_line.message+'\n')
    file.close()

if __name__ == '__main__':
    index = "filebeat-6.5.1-2018.12.02"
    source_log = "/var/log/rocore2.log"
    result_log_path = '/home/us1/git/loglizer' + "/dayco_log.log"
    create_file_from_elastic(index, source_log, result_log_path)
