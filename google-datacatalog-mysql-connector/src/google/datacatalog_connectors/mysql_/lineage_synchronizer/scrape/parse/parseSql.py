import json
from types import SimpleNamespace
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse \
    import lineage
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse.transform_equ.transform_general import TransformGeneral
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

    def parse_query(self, query: str):
        transformer = TransformGeneral()
        query = transformer .transform(query)
        # parse and convert to object
        javaParser = JavaParserConnector()
        query = javaParser.parse_query(query)
        query = str(query)
        jsdata = json.loads(query, object_hook=lambda d: SimpleNamespace(**d))

        # post parse transform
        jsdata = transformer.post_parse_transform(jsdata)
        return lineage.extract_lineage_from_list(jsdata)