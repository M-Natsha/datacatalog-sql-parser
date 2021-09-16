from typing import OrderedDict
from google.datacatalog_connectors.mysql_.lineage_synchronizer.scrape.parse import parseSql
import re

noLineageQueryPatterns = [
    re.compile("\\s*SET\\s+.*", re.IGNORECASE),
    re.compile("\\s*DROP\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*EXPLAIN\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*SHOW\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESC\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESCRIBE\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*DESCRIBE\\s+(.*)", re.IGNORECASE),
    re.compile("\\s*use\\s+(.*)", re.IGNORECASE),
    re.compile("\s*CREATE\s+TABLE((?s).*)\s+ENGINE\s+=\s+[A-Za-z1-9]+;", re.IGNORECASE)
]


class tableLineageExtractor:
    def queryHasLineage(self, query):
        global noLineageQueryPatterns

        for pattern in noLineageQueryPatterns:
            if pattern.match(query):
                return False

        # else it has lineage info
        return True

    def combineLineageEvents(*lineageEvents: dict):
        combinedEvents = {
            'targets' : [],
            'sources': []
        }
        
        for event in lineageEvents:
            if 'targets' in event:
                combinedEvents['targets'] += event['targets']
            
            if 'sources' in event:
                combinedEvents['sources'] += event['sources']
            
        combinedEvents['targets'] = list(OrderedDict.fromkeys(combinedEvents['targets']))
        combinedEvents['sources'] = list(OrderedDict.fromkeys(combinedEvents['sources']))
        
        return combinedEvents
    
    def extractLineageFromTreeNode(self, node):
        if isinstance(node, str):
            return node

        if isinstance(node, list):
            tableArray = []
            for source in node:
                source = self.extractLineageFromTreeNode(source)
                if isinstance(source, list):
                    tableArray += source
                else:
                    tableArray += [source]

            return list(dict.fromkeys(tableArray))

        if 'target' in node and 'source' in node:
            return {
                'target': self.extractLineageFromTreeNode(node['target']),
                'source': self.extractLineageFromTreeNode(node['source'])
            }

        # TODO: unite table format
        if 'tables' in node:
            return self.extractLineageFromTreeNode(node['tables'])

        if 'table' in node:
            return self.extractLineageFromTreeNode(node['table'])

        if 'operation' in node:
            return self.extractLineageFromTreeNode(node['input'])

    def _get_sql_parser(self):
        return parseSql.MySqlParser

    def extract(self, query):
        # Parse sql query
        parser = self._get_sql_parser()()
        parseTree = parser.parseQuery(query)

        # extract lineage
        lineage = self.extractLineageFromTreeNode(parseTree)
        # return lineage
        return lineage
