from connect import connection
from dUtils import changeLogOption, testCreateRandomTable
import subprocess
import os
import re

class MySqlQueryLog:     
    def setup(self):
        con = connection().create()
        mycursor = con.cursor(dictionary=True)
        mycursor.execute("SET global general_log = 1")
        mycursor.execute("SET global log_output = 'table'")
    
    

# setup up the log reading
logReader=MySqlQueryLog()
changeLogOption('ROW')
logReader.setup()

# run an sql query that contains some lineage 
testCreateRandomTable()

# read the logs
data = logReader.readLogs()
for x in data:
   print(x)
   
   

