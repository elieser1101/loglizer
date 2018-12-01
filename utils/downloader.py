# Import Elasticsearch package
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import logging
import json

client = Elasticsearch()
s = Search(using=client, index="filebeat-6.5.1-2018.11.30")
    # .filter("term", system__syslog__hostname="200.74.216.1") \

res = s.scan()
file = open("/home/capuzr/Documents/Deepia/Development/python/Test.log", 'w')

for log in res:
        if "system" in log:
            if "syslog" in log.system:
                file.write(u' '.join((log.system.syslog.timestamp, log.system.syslog.hostname, log.system.syslog.program, log.system.syslog.message, "\n")).encode('utf-8'))
            else:
                if "program" in log.system.auth:
                    file.write(u' '.join((log.system.auth.timestamp, log.system.auth.hostname, log.system.auth.program, log.system.auth.message, "\n")).encode('utf-8'))
