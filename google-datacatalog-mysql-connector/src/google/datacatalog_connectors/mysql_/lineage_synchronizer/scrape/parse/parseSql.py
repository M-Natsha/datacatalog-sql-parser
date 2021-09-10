import subprocess
import os
import json
from types import SimpleNamespace
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse import lineage
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse.javaParserConnector import javaParserConnector

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
        javaParser = javaParserConnector()
        query = javaParser.parseQuery(query)
        query = str(query)
        jsdata = json.loads(query, object_hook=lambda d: SimpleNamespace(**d))
        return lineage.extractLineage(jsdata[0])
