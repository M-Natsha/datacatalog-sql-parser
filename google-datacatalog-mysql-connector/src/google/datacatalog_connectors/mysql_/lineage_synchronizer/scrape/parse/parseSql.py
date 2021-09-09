import subprocess
import os
import json
from types import SimpleNamespace
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse import lineage
import re


ddlRegex = [
    re.compile("\\s*(create)\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*(alter)\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*(drop)\\s+(.*)", re.IGNORECASE)
]


class MySqlParser():

    def queryIsDdl(self):
        return True

    def parseQuery(self, query):
        path2jar = """/home/natsha/work/datacatalog-connectors-rdbms/google-datacatalog-mysql-connector/src/google/datacatalog_connectors/mysql_/lineage_synchronizer/scrape/parse/extractLineage/target/test-1.0-SNAPSHOT-jar-with-dependencies.jar"""
        x = subprocess.check_output(
            ["java", "-jar", path2jar, "-p", query],
            stderr=subprocess.STDOUT)  # doesn't capture output
        # from byte to string
        x = x.decode('ascii')

        # to json
        jsdata = json.loads(x, object_hook=lambda d: SimpleNamespace(**d))
        return lineage.extractLineage(jsdata[0])
