import json
from types import SimpleNamespace
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse \
    import lineage
from .java_parser_connector import JavaParserConnector

import re

ddlRegex = [
    re.compile("\\s*(create)\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*(alter)\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*(drop)\\s+(.*)", re.IGNORECASE)
]


class MySqlParser():

    def queryIsDdl(self):
        return True

    def parse_query(self, query):
        javaParser = JavaParserConnector()
        query = javaParser.parse_query(query)
        query = str(query)
        jsdata = json.loads(query, object_hook=lambda d: SimpleNamespace(**d))
        return lineage.extract_lineage_from_list(jsdata)