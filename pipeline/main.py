import sys
import os

os.chdir('/loglizer')
sys.path.append('/loglizer')#drain __init__.py

# ro1 = open("/rocore1.log")
# ro2 = open("/rocore2.log")

# print(ro1.read())
# print('******')
# print(ro2.read())

os.rename("rocore1", "dayco_log.log")

file = open('/dayco_log.log', 'r')
lines = file.readlines()
file.close()
file = open('/dayco_log.log', 'a')
file.write(lines[-1][:-1])
file.close()
file = open('/dayco_log.log', 'r')
lines = file.readlines()
file.close()

# head /rocore1.log
# print('******')
# head /rocore2.log
